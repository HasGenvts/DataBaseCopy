from typing import List, Dict, Any, Generator
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from src.connectors.base import BaseConnector
from src.models.config import DatabaseConfig
from loguru import logger

class PostgreSQLConnector(BaseConnector):
    def __init__(self, config: DatabaseConfig):
        super().__init__(config)
        self._engine: Engine = None
        
    async def connect(self) -> None:
        try:
            # 构建PostgreSQL连接字符串
            connection_string = (
                f"postgresql+psycopg2://{self.config.username}:{self.config.password}@"
                f"{self.config.host}:{self.config.port}/{self.config.database}"
            )
            
            # 添加schema搜索路径
            if self.config.pg_schema:
                connection_string += f"?options=-csearch_path%3D{self.config.pg_schema}"
            
            self._engine = create_engine(connection_string)
            
            # 测试连接
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"Successfully connected to PostgreSQL database: {self.config.database}")
        except SQLAlchemyError as e:
            logger.error(f"Failed to connect to PostgreSQL database: {str(e)}")
            raise
            
    async def disconnect(self) -> None:
        if self._engine:
            self._engine.dispose()
            logger.info("Disconnected from PostgreSQL database")
            
    async def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        schema = self.config.pg_schema or "public"
        query = """
        SELECT 
            c.column_name,
            c.data_type,
            c.character_maximum_length,
            c.is_nullable,
            (SELECT true 
             FROM information_schema.table_constraints tc
             JOIN information_schema.key_column_usage ku
                ON tc.constraint_name = ku.constraint_name
             WHERE tc.constraint_type = 'PRIMARY KEY'
                AND ku.table_name = :table
                AND ku.table_schema = :schema
                AND ku.column_name = c.column_name
             LIMIT 1
            ) as is_primary_key
        FROM 
            information_schema.columns c
        WHERE 
            c.table_name = :table
            AND c.table_schema = :schema
        ORDER BY 
            c.ordinal_position
        """
        try:
            with self._engine.connect() as conn:
                result = conn.execute(
                    text(query),
                    {"table": table_name, "schema": schema}
                )
                columns = []
                for row in result:
                    columns.append({
                        "name": row.column_name,
                        "type": row.data_type,
                        "length": row.character_maximum_length,
                        "nullable": row.is_nullable == 'YES',
                        "is_primary": row.is_primary_key
                    })
                return {"table_name": table_name, "columns": columns}
        except SQLAlchemyError as e:
            logger.error(f"Failed to get schema for table {table_name}: {str(e)}")
            raise
            
    async def read_data(self, table_name: str, batch_size: int = 1000) -> Generator[List[Dict[str, Any]], None, None]:
        try:
            with self._engine.connect() as conn:
                # 获取总记录数
                schema = self.config.pg_schema or "public"
                count_query = f'SELECT COUNT(*) as count FROM "{schema}"."{table_name}"'
                result = conn.execute(text(count_query))
                total_rows = result.scalar()
                
                # 使用 OFFSET-LIMIT 进行分页
                for offset in range(0, total_rows, batch_size):
                    query = f"""
                    SELECT * FROM "{schema}"."{table_name}"
                    OFFSET :offset
                    LIMIT :limit
                    """
                    result = conn.execute(
                        text(query),
                        {
                            "offset": offset,
                            "limit": batch_size
                        }
                    )
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
                schema = self.config.pg_schema or "public"
                
                # 确保数据是字典列表格式
                processed_data = []
                for item in data:
                    if isinstance(item, dict):
                        processed_data.append(item)
                    elif isinstance(item, (list, tuple)) and len(item) == 1:
                        # 如果是单个值的列表或元组，转换为字典
                        processed_data.append({"name": item[0]})
                    else:
                        logger.warning(f"Skipping invalid data format: {item}")
                        continue
                
                if not processed_data:
                    logger.warning("No valid data to insert")
                    return 0
                
                columns = processed_data[0].keys()
                placeholders = ", ".join([f":{col}" for col in columns])
                column_names = ", ".join(f'"{col}"' for col in columns)
                query = f'INSERT INTO "{schema}"."{table_name}" ({column_names}) VALUES ({placeholders})'
                
                # 执行批量插入
                result = conn.execute(text(query), processed_data)
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
            schema = self.config.schema or "public"
            with self._engine.connect() as conn:
                # 如果是TRUNCATE语句，需要添加schema
                if sql.strip().upper().startswith("TRUNCATE"):
                    table_name = sql.strip().split()[-1]
                    sql = f'TRUNCATE TABLE "{schema}"."{table_name}"'
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
            schema = self.config.schema or "public"
            with self._engine.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) as count FROM "{schema}"."{table_name}"'))
                count = result.scalar()
                logger.debug(f"Table {schema}.{table_name} has {count} rows")
                return count
        except SQLAlchemyError as e:
            logger.error(f"Failed to get row count for table {table_name}: {str(e)}")
            raise 