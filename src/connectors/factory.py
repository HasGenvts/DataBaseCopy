from typing import Dict, Type
from src.connectors.base import BaseConnector
from src.connectors.mysql import MySQLConnector
from src.connectors.sqlserver import SQLServerConnector
from src.connectors.postgresql import PostgreSQLConnector
from src.models.config import DatabaseConfig

class ConnectorFactory:
    _connectors: Dict[str, Type[BaseConnector]] = {
        "mysql": MySQLConnector,
        "sqlserver": SQLServerConnector,
        "postgresql": PostgreSQLConnector
    }
    
    @classmethod
    def get_connector(cls, db_type: str, config: DatabaseConfig) -> BaseConnector:
        """
        获取数据库连接器实例
        
        Args:
            db_type: 数据库类型 (如 "mysql", "sqlserver", "postgresql")
            config: 数据库配置
            
        Returns:
            BaseConnector: 数据库连接器实例
            
        Raises:
            ValueError: 如果数据库类型不支持
        """
        connector_class = cls._connectors.get(db_type.lower())
        if not connector_class:
            raise ValueError(f"Unsupported database type: {db_type}")
            
        return connector_class(config)
    
    @classmethod
    def register_connector(cls, db_type: str, connector_class: Type[BaseConnector]) -> None:
        """
        注册新的数据库连接器
        
        Args:
            db_type: 数据库类型
            connector_class: 连接器类
        """
        cls._connectors[db_type.lower()] = connector_class 