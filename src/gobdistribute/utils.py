import json
from requests import Session
from requests.adapters import HTTPAdapter, Retry
from gobdistribute.config import EXPORT_API_HOST


def json_loads(item):
    try:
        return json.loads(item)
    except Exception as e:
        print(f"ERROR: Deserialization failed for item {item}.")
        raise e


def get_with_retries(url: str):
    session = Session()
    retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    session.mount(EXPORT_API_HOST, HTTPAdapter(max_retries=retries))
    return session.get(url)
