from typing import List, Dict, Any, Generator
import pyodbc
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from src.connectors.base import BaseConnector
from src.models.config import DatabaseConfig
from loguru import logger

class SQLServerConnector(BaseConnector):
    def __init__(self, config: DatabaseConfig):
        super().__init__(config)
        self._engine: Engine = None
        
    async def connect(self) -> None:
        try:
            # 构建SQL Server连接字符串
            driver = self.config.driver or "ODBC Driver 17 for SQL Server"
            trust_cert = "yes" if self.config.trust_server_certificate else "no"
            
            connection_string = (
                f"mssql+pyodbc://{self.config.username}:{self.config.password}@"
                f"{self.config.host}:{self.config.port}/{self.config.database}?"
                f"driver={driver}&TrustServerCertificate={trust_cert}"
            )
            
            self._engine = create_engine(connection_string)
            
            # 测试连接
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"Successfully connected to SQL Server database: {self.config.database}")
        except SQLAlchemyError as e:
            logger.error(f"Failed to connect to SQL Server database: {str(e)}")
            raise
            
    async def disconnect(self) -> None:
        if self._engine:
            self._engine.dispose()
            logger.info("Disconnected from SQL Server database")
            
    async def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        query = """
        SELECT 
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.IS_NULLABLE,
            CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END as IS_PRIMARY_KEY
        FROM 
            INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN (
            SELECT ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                AND ku.TABLE_NAME = :table
        ) pk ON pk.COLUMN_NAME = c.COLUMN_NAME
        WHERE 
            c.TABLE_NAME = :table
        ORDER BY 
            c.ORDINAL_POSITION
        """
        try:
            with self._engine.connect() as conn:
                result = conn.execute(text(query), {"table": table_name})
                columns = []
                for row in result:
                    columns.append({
                        "name": row.COLUMN_NAME,
                        "type": row.DATA_TYPE.lower(),
                        "length": row.CHARACTER_MAXIMUM_LENGTH,
                        "nullable": row.IS_NULLABLE == 'YES',
                        "is_primary": bool(row.IS_PRIMARY_KEY)
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
                
                # 使用 OFFSET-FETCH 进行分页
                for offset in range(0, total_rows, batch_size):
                    query = f"""
                    SELECT * FROM {table_name}
                    ORDER BY (SELECT NULL)
                    OFFSET {offset} ROWS
                    FETCH NEXT {batch_size} ROWS ONLY
                    """
                    result = conn.execute(text(query))
                    batch_data = [dict(row) for row in result]
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