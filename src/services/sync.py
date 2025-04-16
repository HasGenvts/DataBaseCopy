from typing import Dict, Any
from src.models.config import SyncConfig
from src.connectors.factory import ConnectorFactory
from src.connectors.base import BaseConnector
from loguru import logger

class SyncService:
    def __init__(self, config: SyncConfig):
        self.config = config
        self.source_connector: BaseConnector = None
        self.target_connector: BaseConnector = None
        
    async def initialize(self) -> None:
        """初始化源和目标数据库连接"""
        try:
            self.source_connector = ConnectorFactory.get_connector(
                self.config.source.type,
                self.config.source
            )
            self.target_connector = ConnectorFactory.get_connector(
                self.config.target.type,
                self.config.target
            )
            
            await self.source_connector.connect()
            await self.target_connector.connect()
        except Exception as e:
            logger.error(f"Failed to initialize database connections: {str(e)}")
            raise
            
    async def cleanup(self) -> None:
        """清理资源"""
        if self.source_connector:
            await self.source_connector.disconnect()
        if self.target_connector:
            await self.target_connector.disconnect()

    async def truncate_table(self, table_name: str) -> None:
        """清空目标表"""
        try:
            logger.info(f"Truncating target table: {table_name}")
            await self.target_connector.execute(f"TRUNCATE TABLE {table_name}")
            logger.success(f"Successfully truncated table: {table_name}")
        except Exception as e:
            logger.error(f"Failed to truncate table {table_name}: {str(e)}")
            raise

    async def verify_table_data(self, source_table: str, target_table: str) -> bool:
        """验证源表和目标表的数据一致性"""
        try:
            # 获取源表和目标表的行数
            source_count = await self.source_connector.get_row_count(source_table)
            target_count = await self.target_connector.get_row_count(target_table)

            logger.info(f"Verifying data for {source_table} -> {target_table}")
            logger.info(f"Source table count: {source_count}")
            logger.info(f"Target table count: {target_count}")

            if source_count != target_count:
                logger.error(f"Row count mismatch: source={source_count}, target={target_count}")
                return False

            # 可以在这里添加更多的验证逻辑，比如数据采样比对等

            logger.success(f"Data verification passed for {source_table} -> {target_table}")
            return True

        except Exception as e:
            logger.error(f"Failed to verify table data: {str(e)}")
            return False
            
    async def sync_table(self, table_mapping) -> None:
        """
        同步单个表的数据
        
        Args:
            table_mapping: TableMapping对象，包含源表、目标表和字段映射信息
        """
        try:
            # 如果配置了truncate，先清空目标表
            if table_mapping.truncate:
                await self.truncate_table(table_mapping.target)

            # 获取字段映射
            field_mapping = {field.source: field.target for field in table_mapping.fields}
            
            # 分批同步数据
            total_rows = 0
            async for batch in self.source_connector.read_data(table_mapping.source, self.config.batch_size):
                if field_mapping:
                    batch = [self._map_fields(row, field_mapping) for row in batch]
                
                rows_written = await self.target_connector.write_data(table_mapping.target, batch)
                total_rows += rows_written
                logger.info(f"Synced {rows_written} rows to {table_mapping.target}")
                
            logger.success(f"Successfully synced {total_rows} rows from {table_mapping.source} to {table_mapping.target}")

            # 如果配置了verify，验证数据一致性
            if table_mapping.verify:
                if not await self.verify_table_data(table_mapping.source, table_mapping.target):
                    raise ValueError(f"Data verification failed for {table_mapping.source} -> {table_mapping.target}")
            
        except Exception as e:
            logger.error(f"Failed to sync table {table_mapping.source} to {table_mapping.target}: {str(e)}")
            raise
            
    async def sync_all(self) -> None:
        """同步所有配置的表"""
        try:
            await self.initialize()
            
            for table_mapping in self.config.tables:
                logger.info(f"Starting sync from {table_mapping.source} to {table_mapping.target}")
                logger.info(f"Sync options: truncate={table_mapping.truncate}, verify={table_mapping.verify}")
                await self.sync_table(table_mapping)
                
            logger.success("All tables synced successfully")
            
        finally:
            await self.cleanup()
            
    def _validate_schema_compatibility(self, source_schema: Dict[str, Any], target_schema: Dict[str, Any], table_mapping) -> bool:
        """验证源表和目标表的结构兼容性"""
        source_columns = {col["name"]: col for col in source_schema["columns"]}
        target_columns = {col["name"]: col for col in target_schema["columns"]}
        
        # 使用TableMapping中的字段映射
        field_mapping = {field.source: field.target for field in table_mapping.fields}
        
        for source_col_name, source_col in source_columns.items():
            target_col_name = field_mapping.get(source_col_name, source_col_name)
            
            if target_col_name not in target_columns:
                logger.warning(f"Column {target_col_name} not found in target table")
                return False
                
            target_col = target_columns[target_col_name]
            
            # 检查数据类型兼容性
            if not self._are_types_compatible(source_col["type"], target_col["type"]):
                logger.warning(f"Incompatible types: {source_col['type']} -> {target_col['type']}")
                return False
                
        return True
        
    def _are_types_compatible(self, source_type: str, target_type: str) -> bool:
        """检查数据类型是否兼容"""
        # 简单的类型兼容性检查，可以根据需要扩展
        if source_type == target_type:
            return True
            
        # 数值类型兼容性
        numeric_types = {"tinyint", "smallint", "int", "bigint", "float", "double", "decimal"}
        if source_type in numeric_types and target_type in numeric_types:
            return True
            
        # 字符串类型兼容性
        string_types = {"char", "varchar", "text", "mediumtext", "longtext"}
        if source_type in string_types and target_type in string_types:
            return True
            
        return False
        
    def _map_fields(self, row: Dict[str, Any], field_mapping: Dict[str, str]) -> Dict[str, Any]:
        """映射字段名"""
        if not field_mapping:
            return row
            
        return {
            field_mapping.get(k, k): v
            for k, v in row.items()
        } 