# tests/test_src/test_common/test_common.py

from src.common.config import load_config
from src.common.logger import get_logger
from src.common.data_loader import load_json_data

class TestCommon:
    def test_load_config(self):
        config_path = "tests/configs/config-test-mgdb-fwd.yml"
        config = load_config(config_path)
        assert isinstance(config, dict), "Config should be a dictionary"
        assert "mongodb" in config, "Config should contain 'mongodb' key"
        assert "general" in config, "Config should contain 'general' key"
        
        
    def test_load_json_data(self):
        json_path = "tests/data/test_sample_data.json"
        data = load_json_data(json_path)
        assert isinstance(data, list), "Data should be a list"
        assert all(isinstance(item, dict) for item in data)
        assert data[0]["source_id"] == "sfi_smart_ocean;demo;d1;1"

    def test_get_logger(self):
        log_file = "tests/logs/test_logs.log"
        logger = get_logger("test_logger", log_file=log_file)
        
        assert logger is not None, "Logger should not be None"
        assert hasattr(logger, 'info'), "Logger should have 'info' method"
        assert hasattr(logger, 'error'), "Logger should have 'error' method"
        assert hasattr(logger, 'debug'), "Logger should have 'debug' method"
        
        # Test logging functionality
        logger.info("This is an info message")
        logger.error("This is an error message")
        
        # clear the log file after testing
        try:
            with open(log_file, 'w') as f:
                f.write("")
        except Exception as e:
            assert False, f"Logger should log messages without raising exceptions: {e}"