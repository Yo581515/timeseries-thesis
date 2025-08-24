# src/common/config.py

import yaml
import os
from jinja2 import Template

def load_config(config_path: str) -> dict:
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    def env_var(key: str, default=None, required: bool = False):
        val = os.getenv(key, default)
        if (val is None or val == "") and (required or default is None):
            raise KeyError(f"Missing required environment variable: {key}")
        return val

    try:
        with open(config_path, "r") as f:
            template = Template(f.read())
            rendered = template.render(env_var=env_var)
            return yaml.safe_load(rendered) or {}
    except Exception as e:
        raise Exception(f"Error reading config file: {e}")