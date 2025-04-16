from typing import List, Dict, Any, Generator
import pymysql
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from src.connectors.base import BaseConnector
from src.models.config import DatabaseConfig
from loguru import logger

class MySQLConnector(BaseConnector):
    def __init__(self, config: DatabaseConfig):
        super().__init__(config)
        self._engine: Engine = None
        
    async def connect(self) -> None:
        try:
            connection_string = (
                f"mysql+pymysql://{self.config.username}:{self.config.password}@"
                f"{self.config.host}:{self.config.port}/{self.config.database}"
            )
            self._engine = create_engine(connection_string)
            # 测试连接
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"Successfully connected to MySQL database: {self.config.database}")
        except SQLAlchemyError as e:
            logger.error(f"Failed to connect to MySQL database: {str(e)}")
            raise
    
    async def disconnect(self) -> None:
        if self._engine:
            self._engine.dispose()
            logger.info("Disconnected from MySQL database")
    
    async def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, COLUMN_KEY
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = :database AND TABLE_NAME = :table
        ORDER BY ORDINAL_POSITION
        """
        try:
            with self._engine.connect() as conn:
                result = conn.execute(
                    text(query),
                    {"database": self.config.database, "table": table_name}
                )
                columns = []
                for row in result:
                    columns.append({
                        "name": row.COLUMN_NAME,
                        "type": row.DATA_TYPE,
                        "length": row.CHARACTER_MAXIMUM_LENGTH,
                        "nullable": row.IS_NULLABLE == "YES",
                        "is_primary": row.COLUMN_KEY == "PRI"
                    })
                return {"table_name": table_name, "columns": columns}
        except SQLAlchemyError as e:
            logger.error(f"Failed to get schema for table {table_name}: {str(e)}")
            raise
    
    async def read_data(self, table_name: str, batch_size: int = 1000) -> Generator[List[Dict[str, Any]], None, None]:
        try:
            with self._engine.connect() as conn:
                # 获取总记录数
                count_query = f"SELECT COUNT(*) as count FROM {table_name}"
                result = conn.execute(text(count_query))
                total_rows = result.scalar()
                
                # 分批获取数据
                for offset in range(0, total_rows, batch_size):
                    query = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
                    result = conn.execute(text(query))
                    # 确保返回字典格式的数据
                    columns = result.keys()
                    batch_data = []
                    for row in result:
                        row_dict = {}
                        for idx, col in enumerate(columns):
                            row_dict[col] = row[idx]
                        batch_data.append(row_dict)
                    yield batch_data
                    
        except SQLAlchemyError as e:
            logger.error(f"Failed to read data from table {table_name}: {str(e)}")
            raise
    
    async def write_data(self, table_name: str, data: List[Dict[str, Any]]) -> int:
        if not data:
            return 0
            
        try:
            with self._engine.connect() as conn:
                # 构建INSERT语句
                columns = data[0].keys()
                placeholders = ", ".join([f":{col}" for col in columns])
                column_names = ", ".join(columns)
                query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
                
                # 执行批量插入
                result = conn.execute(text(query), data)
                conn.commit()
                return result.rowcount
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to write data to table {table_name}: {str(e)}")
            raise
    
    async def execute_query(self, query: str, params: Dict[str, Any] = None) -> Any:
        try:
            with self._engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return result
        except SQLAlchemyError as e:
            logger.error(f"Failed to execute query: {str(e)}")
            raise

    async def execute(self, sql: str) -> None:
        """
        执行SQL语句
        
        Args:
            sql: 要执行的SQL语句
        """
        try:
            with self._engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
                logger.debug(f"Successfully executed SQL: {sql}")
        except SQLAlchemyError as e:
            logger.error(f"Failed to execute SQL: {sql}, error: {str(e)}")
            raise

    async def get_row_count(self, table_name: str) -> int:
        """
        获取表的总行数
        
        Args:
            table_name: 表名
            
        Returns:
            表中的记录数
        """
        try:
            with self._engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) as count FROM {table_name}"))
                count = result.scalar()
                logger.debug(f"Table {table_name} has {count} rows")
                return count
        except SQLAlchemyError as e:
            logger.error(f"Failed to get row count for table {table_name}: {str(e)}")
            raise 