import logging
from pathlib import Path
from typing import Dict, Any
import yaml

logger = logging.getLogger(__name__)


class ConfigurationManager:

    def __init__(self, config_dir:str = "configs",schema_dir:str = "schemas"):
        self.config_dir = Path(config_dir)
        #self.schema_dir = Path(schema_dir)

        self.config_dir.mkdir(exist_ok=True)
        #self.schema_dir.mkdir(exist_ok=True)



    def load_database_config(self,config_name:str) -> Dict[str, Any]:

        config_path = self.config_dir / config_name
        if not config_path.exists():
            raise FileNotFoundError(f"Config file {config_path} does not exist")
        try:
            with open(config_path,"r") as f:
                config = yaml.safe_load(f)
        except Exception as e:
            raise RuntimeError(f"Failed to load config file {config_path}: {e}")

        return config



