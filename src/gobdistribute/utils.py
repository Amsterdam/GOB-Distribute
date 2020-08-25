import json


def json_loads(item):
    try:
        return json.loads(item)
    except Exception as e:
        print(f"ERROR: Deserialization failed for item {item}.")
        raise e
