import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Optional, Any

import yaml


class DatabaseHandler(ABC):

    def __init__(self, stream_id: Optional[int] = None, config: Optional[Dict[str, Any]] = None):

        #self.config_dir = os.path.join(os.getcwd(), "configs")
        self.config = config
        self.stream_id = stream_id

    @abstractmethod
    def get_connection(self):
        pass

    @abstractmethod
    async def execute_query(self, query,parameters):
        pass

    @abstractmethod
    async def load_data(self,csv_file: Path, table_name: str):
        pass

    @abstractmethod
    async def execute_ddl(self,sql:str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    async def initialize_connection_pool(self, config) -> None:
        """Dediacated connection pool for each database handler"""




