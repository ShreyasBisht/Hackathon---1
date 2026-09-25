import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

URL = "http://127.0.0.1:8000"

def upload(i):
    filename = f"concurrent_{i}.txt"
    content = f"Concurrent test object {i}"

    try:
        r = requests.post(
            f"{URL}/api/upload",
            files={"file": (filename, content.encode())},
            timeout=10
        )
        return i, r.status_code, r.text[:100]
    except Exception as e:
        return i, "ERROR", str(e)

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(upload, i) for i in range(20)]

    for future in as_completed(futures):
        print(future.result())

print("\nConcurrency test complete.")