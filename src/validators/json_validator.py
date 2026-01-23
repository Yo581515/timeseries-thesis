import json

def is_str_valid_json(text: str) -> bool:
    try:
        json.loads(text)
        return True
    except Exception:
        return False


def is_obj_valid_json(obj) -> bool:
    try:
        json.dumps(obj)
        return True
    except Exception:
        return False
