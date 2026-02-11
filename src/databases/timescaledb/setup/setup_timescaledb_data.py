# src/databases/timescaledb/setup/setup_timescaledb_data.py

import json
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union
import pandas as pd

from src.common.config import load_config
from src.common.logger import get_logger
from src.common.data_loader import load_json_data
from src.databases.timescaledb.config import get_timescaledb_config

JsonDoc = Dict[str, Any]


def parse_time(value: Any) -> Optional[datetime]:
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


def get_time_range_yyyymmdd(events: List[JsonDoc]) -> Tuple[str, str]:
    times: List[datetime] = []
    for e in events:
        dt = parse_time(e.get("time"))
        if dt:
            times.append(dt)

    if not times:
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        return today, today

    return min(times).strftime("%Y%m%d"), max(times).strftime("%Y%m%d")


def extract_row_index(filename: str) -> Optional[str]:
    m = re.search(r"row_data_(\d+)\.json$", filename)
    return m.group(1) if m else None


def coerce_to_events(obj: Any) -> List[JsonDoc]:
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        return [obj]
    return []


def events_to_dataframe(events: List[JsonDoc]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for e in events:
        t = parse_time(e.get("time"))
        if t is None:
            continue

        node_source = e.get("source")
        node_source_id = e.get("source_id")

        loc = e.get("location") or {}
        latitude = loc.get("latitude")
        longitude = loc.get("longitude")

        observations = e.get("observations") or []
        if not isinstance(observations, list):
            continue

        for o in observations:
            if not isinstance(o, dict):
                continue

            rows.append({
                "time": t,
                "node_source": node_source,
                "node_source_id": node_source_id,
                "latitude": latitude,
                "longitude": longitude,
                "sensor_source": o.get("source"),
                "sensor_source_id": o.get("source_id"),
                "parameter": o.get("parameter"),
                "value": o.get("value"),
                "unit": o.get("unit"),
                "quality_codes": o.get("qualityCodes"),
            })

    df = pd.DataFrame(rows)

    # Ensure consistent dtypes where practical
    if not df.empty:
        # Keep 'quality_codes' as object (list) for parquet
        df["time"] = pd.to_datetime(df["time"], utc=True)

    return df


def export_parquet(events: List[JsonDoc], out_path: Union[str, Path]) -> int:
    df = events_to_dataframe(events)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(out_path, index=False)
    return len(df)


if __name__ == "__main__":
    config_file_path = "./configs/config-timescaledb.yml"
    config = load_config(config_file_path)

    logger = get_logger("setup_timescaledb_data.py", config["general"]["log_file"])
    _timescaledb_config = get_timescaledb_config(config["database"])

    entity = "data"
    phenomenon = "measurements"

    row_data_folder = Path("data/row_data")
    out_folder = Path(config["general"]["timescaledb_data_folder_path"])
    out_folder.mkdir(parents=True, exist_ok=True)

    json_files = sorted(row_data_folder.glob("row_data_*.json"))
    if not json_files:
        raise SystemExit(f"No matching files found in {row_data_folder.resolve()}")

    for fp in json_files:
        idx = extract_row_index(fp.name) or fp.stem
        logger.info(f"Processing {fp.name} (idx={idx})")

        try:
            raw_obj = load_json_data(str(fp))
        except Exception as e:
            logger.error(f"Failed reading {fp.name}: {e}")
            continue

        events = coerce_to_events(raw_obj)
        if not events:
            logger.warning(f"{fp.name}: no events found (empty or unexpected JSON root)")
            continue

        start, end = get_time_range_yyyymmdd(events)

        out_name = f"{entity}_{phenomenon}_{idx}_{start}_{end}.parquet"
        out_path = out_folder / out_name

        try:
            row_count = export_parquet(events, out_path)
        except Exception as e:
            logger.error(f"{fp.name}: failed writing parquet: {e}")
            continue

        logger.info(f"Saved {row_count} rows -> {out_path.resolve()}")

    logger.info("Done.")
