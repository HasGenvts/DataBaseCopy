from typing import Dict, List, Optional
from dataclasses import dataclass

@dataclass
class DatabaseConfig:
    type: str
    host: str
    port: int
    username: str
    password: str
    database: str
    driver: Optional[str] = None
    schema: Optional[str] = None
    sslmode: Optional[str] = None
    trust_server_certificate: Optional[bool] = None

@dataclass
class FieldMapping:
    source: str
    target: str
    transform: Optional[str] = None

@dataclass
class TableMapping:
    source: str
    target: str
    fields: List[FieldMapping]
    truncate: bool = False
    verify: bool = True

@dataclass
class SyncConfig:
    source: DatabaseConfig
    target: DatabaseConfig
    tables: List[TableMapping]
    batch_size: int = 1000
    max_concurrent_tasks: int = 5
    verify_data: bool = True
    retry_times: int = 3
    retry_interval: int = 5 