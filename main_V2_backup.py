
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from pathlib import Path
import sqlite3, hashlib, shutil, uuid, time, asyncio, html, threading, os

BASE = Path(__file__).resolve().parent
STORAGE = BASE / "storage"
DB = BASE / "metadata.db"
RF = 3
STATE_LOCK = threading.RLock()
NODES = [f"node{i}" for i in range(1, 6)]

app = FastAPI(title="Vault - Fault-Tolerant Distributed Object Storage")

HTML = r"""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Vault</title>
<style>
*{box-sizing:border-box}body{margin:0;background:#0b1020;color:#edf2ff;font-family:Segoe UI,Arial,sans-serif}
header{padding:24px 7%;background:#111a31;border-bottom:1px solid #263352;display:flex;justify-content:space-between;align-items:center}
h1{margin:0;letter-spacing:6px;font-size:34px}header p{margin:5px 0 0;color:#98a8c8}.badge{border:1px solid #344363;padding:9px 14px;border-radius:20px}
main{width:86%;max-width:1200px;margin:28px auto}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:14px}
.card,.panel{background:#121b31;border:1px solid #273657;border-radius:14px;padding:20px}.card span{color:#91a0bf}.card strong{display:block;font-size:27px;margin-top:8px}
.panel{margin-top:18px}.panel h2{margin-top:0;font-size:19px}.row{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
input[type=file]{padding:10px;background:#0d1528;border:1px solid #30405f;border-radius:8px;color:#dbe4fa}
button{border:0;border-radius:8px;padding:10px 14px;background:#4f7cff;color:white;font-weight:600;cursor:pointer}
button:hover{opacity:.9}button.danger{background:#c84b5b}button.good{background:#3b9d70}button.secondary{background:#34415f}
.nodes{display:grid;grid-template-columns:repeat(5,1fr);gap:12px}.node{padding:16px;border:1px solid #2c3b5d;border-radius:10px;background:#0e1629}
.dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:7px;background:#e05a68}.online .dot{background:#46c98a}
.small{font-size:12px;color:#92a0bd}.file{display:flex;justify-content:space-between;gap:15px;align-items:center;padding:13px 0;border-bottom:1px solid #26324d}.file:last-child{border-bottom:0}
.tags{display:flex;gap:5px;flex-wrap:wrap;margin-top:7px}.tag{font-size:11px;padding:4px 7px;border-radius:10px;background:#202d49}.ok{color:#55d69b}.bad{color:#ff7584}
.log{height:150px;overflow:auto;background:#0a1120;border-radius:8px;padding:12px;font:12px Consolas,monospace;color:#a9bad8}
.msg{margin-top:10px;color:#9fb0d2}.empty{color:#7f8ca8;padding:15px 0}
@media(max-width:800px){.cards{grid-template-columns:repeat(2,1fr)}.nodes{grid-template-columns:repeat(2,1fr)}.file{align-items:flex-start;flex-direction:column}}
</style>
</head>
<body>
<header><div><h1>VAULT</h1><p>Fault-Tolerant Distributed Object Storage</p></div><div class="row"><label class="badge">Replication: <select id="rf" onchange="setRF(this.value)" style="background:transparent;color:inherit;border:0;font-weight:700"><option>1</option><option>2</option><option>3</option><option>4</option><option>5</option></select>×</label></div></header>
<main>
<section class="cards">
<div class="card"><span>Healthy Nodes</span><strong id="healthy">-</strong></div>
<div class="card"><span>Objects</span><strong id="objects">-</strong></div>
<div class="card"><span>Healthy Replicas</span><strong id="replicas">-</strong></div>
<div class="card"><span>System</span><strong class="ok" id="system">ONLINE</strong></div>
</section>
<section class="panel"><h2>Upload Object</h2><div class="row"><input id="file" type="file"><button onclick="upload()">Upload & Replicate</button><button class="secondary" onclick="integrity()">Verify Integrity</button><button class="good" onclick="rebalance()">Rebalance</button></div><div id="msg" class="msg"></div></section>
<section class="panel"><h2>Storage Nodes</h2><div id="nodes" class="nodes"></div></section>
<section class="panel"><h2>Stored Objects</h2><div id="files"></div></section>
<section class="panel"><h2>Event Log</h2><div id="log" class="log"></div></section>
</main>
<script>
const logEl=document.getElementById('log');
function log(s){const d=document.createElement('div');d.textContent=new Date().toLocaleTimeString()+"  "+s;logEl.prepend(d)}
async function api(url,opt={}){const r=await fetch(url,opt);let x;try{x=await r.json()}catch{x={}}if(!r.ok)throw new Error(x.detail||"Request failed");return x}
async function refresh(){
 const [ns,fs,cfg]=await Promise.all([api('/api/nodes'),api('/api/files'),api('/api/config')]);
 document.getElementById('rf').value=cfg.replication_factor;
 const healthy=ns.filter(n=>n.status==='online').length;
 document.getElementById('healthy').textContent=healthy+"/5";
 document.getElementById('objects').textContent=fs.length;
 document.getElementById('replicas').textContent=fs.reduce((a,f)=>a+f.healthy.length,0);
 document.getElementById('system').textContent=healthy>=3?'ONLINE':'DEGRADED';
 document.getElementById('system').className=healthy>=3?'ok':'bad';
 document.getElementById('nodes').innerHTML=ns.map(n=>`
 <div class="node ${n.status==='online'?'online':''}">
 <b><span class="dot"></span>${n.id}</b><p class="small">${n.status.toUpperCase()}</p>
 ${n.status==='online'
 ? `<button class="secondary" onclick="partitionNode('${n.id}')">Partition</button> `
 : ''}
 ${n.status==='online'
 ? `<button class="danger" onclick="failNode('${n.id}')">Simulate Failure</button>`
 : `<button class="good" onclick="recoverNode('${n.id}')">Recover Node</button>`}
 </div>`).join('');
 document.getElementById('files').innerHTML=fs.length?fs.map(f=>`
 <div class="file"><div><b>${escapeHtml(f.filename)}</b>
 <div class="small">SHA-256: ${f.checksum}... | Replicas: ${f.healthy.length}/${f.replicas.length}</div>
 <div class="tags">${f.replicas.map(n=>`<span class="tag ${f.healthy.includes(n)?'ok':'bad'}">${n} ${f.healthy.includes(n)?'✓':'✕'}</span>`).join('')}</div></div>
 <div class="row"><button onclick="downloadFile('${f.id}','${escapeAttr(f.filename)}')">Download</button>
 <button class="danger" onclick="corrupt('${f.id}','${f.healthy[0]||f.replicas[0]}')">Corrupt Replica</button>
 <button class="good" onclick="repair('${f.id}')">Repair</button><button class="danger" onclick="deleteObject('${f.id}')">Delete</button></div></div>`).join(''):'<div class="empty">No objects uploaded yet.</div>';
}
function escapeHtml(s){return s.replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
function escapeAttr(s){return s.replace(/'/g,"\\'")}
async function upload(){
 const f=document.getElementById('file').files[0]; if(!f){alert('Choose a file first.');return}
 const fd=new FormData();fd.append('file',f);
 try{const x=await api('/api/upload',{method:'POST',body:fd});document.getElementById('msg').textContent=`Uploaded and replicated to ${x.replicas.join(', ')}`;log(`UPLOAD ${x.filename} → ${x.replicas.join(', ')}`);refresh()}catch(e){alert(e.message)}
}
async function setRF(v){try{await api('/api/config/replication',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({replication_factor:Number(v)})});log(`REPLICATION FACTOR → ${v}×`);refresh()}catch(e){alert(e.message)}}
async function partitionNode(n){try{await api('/api/nodes/'+n+'/partition',{method:'POST'});log(`PARTITION simulated: ${n}`);refresh()}catch(e){alert(e.message)}}
async function failNode(n){try{await api('/api/nodes/'+n+'/fail',{method:'POST'});log(`FAILURE simulated: ${n}`);refresh()}catch(e){alert(e.message)}}
async function recoverNode(n){try{await api('/api/nodes/'+n+'/recover',{method:'POST'});log(`NODE RECOVERED: ${n}`);refresh()}catch(e){alert(e.message)}}
async function corrupt(id,n){if(!n){alert('No replica available');return}try{const x=await api(`/api/nodes/${n}/corrupt/${id}`,{method:'POST'});log(`CORRUPTION injected on ${n}`);alert(x.message);refresh()}catch(e){alert(e.message)}}
async function repair(id){try{const x=await api('/api/repair/'+id,{method:'POST'});log(`REPAIR: ${x.repaired.length?x.repaired.join(', '):'no repair needed'}`);refresh()}catch(e){alert(e.message)}}
async function rebalance(){try{const x=await api('/api/rebalance',{method:'POST'});log(`REBALANCE: ${x.repaired} replica repairs`);refresh()}catch(e){alert(e.message)}}
async function deleteObject(id){if(!confirm('Delete this object and all replicas?'))return;try{await api('/api/objects/'+id,{method:'DELETE'});log(`DELETE object ${id}`);refresh()}catch(e){alert(e.message)}}
async function integrity(){try{const x=await api('/api/integrity');x.forEach(r=>log(`${r.healthy?'OK':'CORRUPTED'}: ${r.filename} @ ${r.node}`));alert('Integrity verification complete. Check Event Log.');refresh()}catch(e){alert(e.message)}}
async function downloadFile(id,name){const r=await fetch('/api/download/'+id);if(!r.ok){const x=await r.json();alert(x.detail||'Download failed');return}const b=await r.blob();const a=document.createElement('a');a.href=URL.createObjectURL(b);a.download=name;a.click();URL.revokeObjectURL(a.href);log(`DOWNLOAD ${name} successful`)}
refresh();setInterval(refresh,4000);
</script>
</body>
</html>
"""

def connect():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    return con

def init():
    STORAGE.mkdir(exist_ok=True)
    for n in NODES:
        (STORAGE/n).mkdir(parents=True, exist_ok=True)
    con=connect()
    con.execute("CREATE TABLE IF NOT EXISTS nodes(id TEXT PRIMARY KEY,status TEXT NOT NULL)")
    con.execute("""CREATE TABLE IF NOT EXISTS objects(
        id TEXT PRIMARY KEY,filename TEXT NOT NULL,size INTEGER NOT NULL,
        checksum TEXT NOT NULL,created REAL NOT NULL)""")
    con.execute("""CREATE TABLE IF NOT EXISTS replicas(
        object_id TEXT NOT NULL,node_id TEXT NOT NULL,
        PRIMARY KEY(object_id,node_id))""")
    for n in NODES:
        con.execute("INSERT OR IGNORE INTO nodes VALUES(?,?)",(n,"online"))
    con.commit(); con.close()

def status(n):
    con=connect(); r=con.execute("SELECT status FROM nodes WHERE id=?",(n,)).fetchone(); con.close()
    return r["status"] if r else "failed"

def set_status(n,s):
    con=connect(); con.execute("UPDATE nodes SET status=? WHERE id=?",(s,n)); con.commit(); con.close()

def checksum(p):
    h=hashlib.sha256()
    with open(p,"rb") as f:
        for chunk in iter(lambda:f.read(1024*1024),b""): h.update(chunk)
    return h.hexdigest()

def obj(oid):
    con=connect(); r=con.execute("SELECT * FROM objects WHERE id=?",(oid,)).fetchone(); con.close(); return r

def reps(oid):
    con=connect(); rows=con.execute("SELECT node_id FROM replicas WHERE object_id=?",(oid,)).fetchall(); con.close()
    return [r["node_id"] for r in rows]

def add_rep(oid,n):
    con=connect(); con.execute("INSERT OR IGNORE INTO replicas VALUES(?,?)",(oid,n)); con.commit(); con.close()

def repair(oid):
    o=obj(oid)
    if not o: return {"ok":False,"message":"Object not found"}
    good=[]; bad=[]
    for n in reps(oid):
        p=STORAGE/n/oid
        if status(n)=="online" and p.exists() and checksum(p)==o["checksum"]: good.append(n)
        else: bad.append(n)
    if not good: return {"ok":False,"message":"No healthy source replica"}
    source=good[0]; repaired=[]
    # Replace corrupted online replicas.
    for n in bad:
        if status(n)=="online":
            shutil.copy2(STORAGE/source/oid, STORAGE/n/oid); repaired.append(n); good.append(n)
    # Rebuild RF on other online nodes when a node has failed.
    candidates=[n for n in NODES if status(n)=="online" and n not in good]
    while len(good)<RF and candidates:
        target=candidates.pop(0)
        shutil.copy2(STORAGE/source/oid, STORAGE/target/oid)
        add_rep(oid,target); good.append(target); repaired.append(target)
    return {"ok":True,"repaired":repaired,"healthy_replicas":good}

@app.get("/api/config")
def config():
    return {"replication_factor": RF, "node_count": len(NODES)}

@app.post("/api/config/replication")
def set_replication(payload: dict):
    global RF
    value = int(payload.get("replication_factor", RF))
    if value < 1 or value > len(NODES): raise HTTPException(400, f"Replication factor must be between 1 and {len(NODES)}")
    RF = value
    return {"replication_factor": RF}

@app.on_event("startup")
async def startup():
    init()
    asyncio.create_task(worker())

async def worker():
    while True:
        await asyncio.sleep(5)
        con=connect(); ids=[r["id"] for r in con.execute("SELECT id FROM objects").fetchall()]; con.close()
        for oid in ids: repair(oid)

@app.get("/",response_class=HTMLResponse)
def home(): return HTML

@app.get("/api/nodes")
def nodes():
    con=connect(); rows=con.execute("SELECT * FROM nodes ORDER BY id").fetchall(); con.close()
    return [dict(r) for r in rows]

@app.get("/api/files")
def files():
    con=connect(); rows=con.execute("SELECT * FROM objects ORDER BY created DESC").fetchall(); con.close()
    out=[]
    for o in rows:
        rr=reps(o["id"]); healthy=[]
        for n in rr:
            p=STORAGE/n/o["id"]
            if status(n)=="online" and p.exists() and checksum(p)==o["checksum"]: healthy.append(n)
        out.append({"id":o["id"],"filename":o["filename"],"size":o["size"],
                    "checksum":o["checksum"],"replicas":rr,"healthy":healthy})
    return out

@app.post("/api/upload")
async def upload(file:UploadFile=File(...)):
    oid=uuid.uuid4().hex
    temp=STORAGE/("_temp_"+oid)
    with open(temp,"wb") as out:
        while True:
            chunk=await file.read(1024*1024)
            if not chunk: break
            out.write(chunk)
    if temp.stat().st_size == 0:
        temp.unlink(missing_ok=True)
        raise HTTPException(400,"Empty file")
    data_size=temp.stat().st_size
    online=[n for n in NODES if status(n)=="online"]
    if len(online)<RF: raise HTTPException(503,f"Need at least {RF} healthy nodes")
    cs=checksum(temp)
    chosen=online[:RF]
    with STATE_LOCK:
        for n in chosen: shutil.copy2(temp,STORAGE/n/oid)
        temp.unlink(missing_ok=True)
        filename=Path(file.filename or "object").name
        con=connect()
        con.execute("INSERT INTO objects VALUES(?,?,?,?,?)",(oid,filename,data_size,cs,time.time()))
        for n in chosen: con.execute("INSERT INTO replicas VALUES(?,?)",(oid,n))
        con.commit(); con.close()
    return {"message":"Upload successful","object_id":oid,"filename":filename,"replicas":chosen,"checksum":cs}

@app.get("/api/download/{oid}")
def download(oid:str):
    o=obj(oid)
    if not o: raise HTTPException(404,"Object not found")
    for n in reps(oid):
        p=STORAGE/n/oid
        if status(n)=="online" and p.exists() and checksum(p)==o["checksum"]:
            return FileResponse(p,filename=o["filename"],headers={"X-Vault-Source-Node":n})
    raise HTTPException(503,"No healthy replica available")

@app.post("/api/nodes/{n}/partition")
def partition(n:str):
    if n not in NODES: raise HTTPException(404,"Node not found")
    set_status(n,"partitioned"); return {"status":"partitioned","node":n}

@app.post("/api/nodes/{n}/fail")
def fail(n:str):
    if n not in NODES: raise HTTPException(404,"Node not found")
    set_status(n,"failed"); return {"status":"failed","node":n}

@app.post("/api/nodes/{n}/recover")
def recover(n:str):
    if n not in NODES: raise HTTPException(404,"Node not found")
    set_status(n,"online"); return {"status":"online","node":n}

@app.post("/api/nodes/{n}/corrupt/{oid}")
def corrupt(n:str,oid:str):
    if n not in NODES or status(n)!="online": raise HTTPException(400,"Node is not online")
    if n not in reps(oid): raise HTTPException(404,"Replica not found")
    p=STORAGE/n/oid
    if not p.exists(): raise HTTPException(404,"File missing")
    with open(p,"ab") as f: f.write(b"\nVAULT_CORRUPTED")
    return {"message":"Corruption injected"}

@app.post("/api/repair/{oid}")
def manual_repair(oid:str): return repair(oid)

@app.post("/api/rebalance")
def rebalance():
    con=connect(); ids=[r["id"] for r in con.execute("SELECT id FROM objects").fetchall()]; con.close()
    total=0
    for oid in ids:
        total += len(repair(oid).get("repaired", []))
    return {"ok":True,"repaired":total}

@app.delete("/api/objects/{oid}")
def delete_object(oid:str):
    if not obj(oid): raise HTTPException(404,"Object not found")
    with STATE_LOCK:
        for n in reps(oid):
            (STORAGE/n/oid).unlink(missing_ok=True)
        con=connect()
        con.execute("DELETE FROM replicas WHERE object_id=?",(oid,))
        con.execute("DELETE FROM objects WHERE id=?",(oid,))
        con.commit(); con.close()
    return {"ok":True}

@app.get("/api/integrity")
@app.post("/api/integrity")
def integrity():
    con=connect(); ids=[r["id"] for r in con.execute("SELECT id FROM objects").fetchall()]; con.close()
    result=[]
    for oid in ids:
        o=obj(oid)
        for n in reps(oid):
            p=STORAGE/n/oid
            good=status(n)=="online" and p.exists() and checksum(p)==o["checksum"]
            result.append({"filename":o["filename"],"node":n,"healthy":good})
    return result
