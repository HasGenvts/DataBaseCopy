import logging
from typing import Dict, Any
from src.models.config import Config, TableMapping
from src.db.connection import get_connection

logger = logging.getLogger(__name__)

class DatabaseSynchronizer:
    def __init__(self, config: Config):
        self.config = config
        self.source_conn = get_connection(config.source)
        self.target_conn = get_connection(config.target)
    
    def sync_table(self, mapping: TableMapping) -> bool:
        try:
            source_cursor = self.source_conn.cursor()
            target_cursor = self.target_conn.cursor()
            
            # 如果配置了清空目标表
            if self.config.options.truncate_target:
                logger.info(f"Truncating target table: {mapping.target_table}")
                target_cursor.execute(f"TRUNCATE TABLE {mapping.target_table}")
            
            # 构建查询语句
            source_fields = ", ".join(mapping.field_mappings.keys())
            target_fields = ", ".join(mapping.field_mappings.values())
            
            # 分批读取源数据
            source_cursor.execute(f"SELECT {source_fields} FROM {mapping.source_table}")
            batch = []
            
            while True:
                rows = source_cursor.fetchmany(self.config.options.batch_size)
                if not rows:
                    break
                    
                batch.extend(rows)
                if len(batch) >= self.config.options.batch_size:
                    self._insert_batch(target_cursor, mapping.target_table, target_fields, batch)
                    batch = []
            
            # 插入最后一批数据
            if batch:
                self._insert_batch(target_cursor, mapping.target_table, target_fields, batch)
            
            # 如果配置了验证行数
            if self.config.options.verify_row_count:
                if not self._verify_row_count(mapping):
                    logger.error(f"Row count verification failed for table {mapping.source_table}")
                    return False
            
            self.target_conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Error syncing table {mapping.source_table}: {str(e)}")
            self.target_conn.rollback()
            return False
    
    def _insert_batch(self, cursor: Any, table: str, fields: str, batch: list):
        placeholders = ", ".join(["%s"] * len(batch[0]))
        insert_sql = f"INSERT INTO {table} ({fields}) VALUES ({placeholders})"
        cursor.executemany(insert_sql, batch)
    
    def _verify_row_count(self, mapping: TableMapping) -> bool:
        source_cursor = self.source_conn.cursor()
        target_cursor = self.target_conn.cursor()
        
        source_cursor.execute(f"SELECT COUNT(*) FROM {mapping.source_table}")
        source_count = source_cursor.fetchone()[0]
        
        target_cursor.execute(f"SELECT COUNT(*) FROM {mapping.target_table}")
        target_count = target_cursor.fetchone()[0]
        
        if source_count != target_count:
            logger.error(f"Row count mismatch for table {mapping.source_table}:")
            logger.error(f"Source: {source_count}, Target: {target_count}")
            return False
            
        logger.info(f"Row count verified for table {mapping.source_table}: {source_count} rows")
        return True
    
    def sync_all(self) -> bool:
        success = True
        for mapping in self.config.table_mappings:
            if not self.sync_table(mapping):
                success = False
        return success
    
    def close(self):
        self.source_conn.close()
        self.target_conn.close() 