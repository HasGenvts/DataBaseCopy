from abc import ABC, abstractmethod
from typing import List, Dict, Any, Generator
from src.models.config import DatabaseConfig

class BaseConnector(ABC):
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._connection = None
    
    @abstractmethod
    async def connect(self) -> None:
        """建立数据库连接"""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """关闭数据库连接"""
        pass
    
    @abstractmethod
    async def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """获取表结构"""
        pass
    
    @abstractmethod
    async def read_data(self, table_name: str, batch_size: int = 1000) -> Generator[List[Dict[str, Any]], None, None]:
        """分批读取数据"""
        pass
    
    @abstractmethod
    async def write_data(self, table_name: str, data: List[Dict[str, Any]]) -> int:
        """写入数据，返回写入的记录数"""
        pass
    
    @abstractmethod
    async def execute_query(self, query: str, params: Dict[str, Any] = None) -> Any:
        """执行自定义查询"""
        pass

    @abstractmethod
    async def execute(self, sql: str) -> None:
        """
        执行SQL语句
        
        Args:
            sql: 要执行的SQL语句
        """
        pass

    @abstractmethod
    async def get_row_count(self, table_name: str) -> int:
        """
        获取表的总行数
        
        Args:
            table_name: 表名
            
        Returns:
            表中的记录数
        """
        pass 