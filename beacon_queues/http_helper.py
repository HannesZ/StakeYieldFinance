import time
import requests

def get_with_retries(url, params=None, retries=3, backoff=1.5, timeout=20):
    attempt = 0
    while True:
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                return r
            attempt += 1
            if attempt > retries:
                print(f"HTTP {r.status_code} for {url} with {params}. Giving up.")
                return None
            time.sleep(backoff ** attempt)
        except requests.RequestException as e:
            attempt += 1
            if attempt > retries:
                print(f"Request error for {url} with {params}: {e}. Giving up.")
                return None
            time.sleep(backoff ** attempt)
