# src/common/config.py

import yaml
import os

def load_config(config_path) -> dict:
    """
    Loads YAML config file from the given path.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    try:
        with open(config_path, "r") as file:
            return yaml.safe_load(file)
    except Exception as e:
        raise Exception(f"Error reading config file: {e}")
