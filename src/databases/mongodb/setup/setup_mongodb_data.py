# src/databases/mongodb/setup/setup_mongodb_data.py

import json
import re
from pathlib import Path
from pprint import pprint
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

from src.common.config import load_config
from src.common.logger import get_logger
from src.common.data_loader import load_json_data

from src.databases.mongodb.config import get_mongodb_config
from src.databases.mongodb.utils.data_utils import resolve_data, make_strftime_from_utc
from src.validators.json_validator import is_obj_valid_json


JsonDoc = Dict[str, Any]


def save_json(data: Any, out_path: Union[str, Path]) -> None:
    out_fp = Path(out_path)
    out_fp.parent.mkdir(parents=True, exist_ok=True)
    with out_fp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def parse_iso_time(value: Any) -> Optional[datetime]:
    """
    Parse time values that may be:
      - ISO string (with/without timezone, with Z)
      - datetime
    Returns timezone-aware UTC datetime or None.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None
    else:
        return None

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def get_time_range_yyyymmdd(docs: List[JsonDoc]) -> Tuple[str, str]:
    times: List[datetime] = []
    for d in docs:
        dt = parse_iso_time(d.get("time"))
        if dt:
            times.append(dt)

    if not times:
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        return today, today

    return min(times).strftime("%Y%m%d"), max(times).strftime("%Y%m%d")


def extract_row_index(filename: str) -> Optional[str]:
    """
    row_data_10.json -> "10"
    returns None if not found
    """
    m = re.search(r"row_data_(\d+)\.json$", filename)
    return m.group(1) if m else None


def coerce_to_docs(obj: Any) -> List[JsonDoc]:
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        return [obj]
    return []


if __name__ == "__main__":
    config_file_path = "./configs/config-mgdb.yml"
    config = load_config(config_file_path)

    logger = get_logger("setup_mongodb_data.py", config["general"]["log_file"])
    _mongodb_config = get_mongodb_config(config["database"])

    # -------------------------
    # Configurable naming pieces
    # -------------------------
    entity = "data"
    phenomenon = "measurements"

    # -------------------------
    # Input/Output folders
    # -------------------------
    row_data_folder = Path("data/row_data")
    out_folder = Path("data/mongodb_data")
    out_folder.mkdir(parents=True, exist_ok=True)

    json_files = sorted(row_data_folder.glob("row_data_*.json"))
    if not json_files:
        raise SystemExit(f"No matching files found in {row_data_folder.resolve()}")

    for fp in json_files:
        idx = extract_row_index(fp.name) or fp.stem  # fallback if pattern differs
        logger.info(f"Processing {fp.name} (idx={idx})")

        try:
            raw_obj = load_json_data(str(fp))
        except Exception as e:
            logger.error(f"Failed reading {fp.name}: {e}")
            continue

        raw_docs = coerce_to_docs(raw_obj)
        if not raw_docs:
            logger.warning(f"{fp.name}: no docs found (empty or unexpected JSON root)")
            continue

        resolved_docs: List[JsonDoc] = []
        dropped = 0

        for doc in raw_docs:
            if not resolve_data(doc, logger):
                dropped += 1
                continue

            # convert datetime -> ISO string for JSON output
            make_strftime_from_utc(doc)

            if not is_obj_valid_json(doc):
                logger.error(f"{fp.name}: doc not JSON-valid after resolving; dropping it.")
                dropped += 1
                continue

            resolved_docs.append(doc)

        if not resolved_docs:
            logger.warning(f"{fp.name}: all docs dropped (dropped={dropped})")
            continue

        start, end = get_time_range_yyyymmdd(resolved_docs)

        # output file name: data_observations_10_20240101_20240131.json
        out_name = f"{entity}_{phenomenon}_{idx}_{start}_{end}.json"
        out_path = out_folder / out_name

        save_json(resolved_docs, out_path)

        logger.info(
            f"Saved {len(resolved_docs)} docs (dropped={dropped}) -> {out_path.resolve()}"
        )

    logger.info("Done.")



# if __name__ == "__main__":
#     config_file_path = "./configs/config-mgdb-fwd.yml"
#     config = load_config(config_file_path)

#     logger = get_logger("setup_mongodb_data.py", config["general"]["log_file"])

#     mongodb_config = get_mongodb_config(config["mongodb"])

#     data_file = load_json_data("data/row_data/row_data_1.json")

#     # optional cleanup/formatting
#     data = [doc for doc in data_file if resolve_data(doc, logger)]
    
#     mdb_data = [doc for doc in data_file if make_strftime_from_utc(doc)]
    
#     pprint(mdb_data[0])
#     print(is_obj_valid_json(mdb_data[0]))
    
    
    
#     # 1 get data
#     mongodb_row_data_folder_path = "data/mongodb_data"
    
    
    
    
    
    
#     # 2 resolve data
    
    
    
    
#     # 3 save data
#     mongodb_resolved_data_folder_path = "data/mongodb_data"
    
     