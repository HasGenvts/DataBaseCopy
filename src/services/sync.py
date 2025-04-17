from typing import Dict, Any
import asyncio
import time
from src.models.config import SyncConfig, TableMapping
from src.connectors.factory import ConnectorFactory
from src.connectors.base import BaseConnector
from loguru import logger
from src.services.batch_processor import MultiProcessSync

class SyncService:
    def __init__(self, config: SyncConfig):
        self.config = config
        self.source_connector: BaseConnector = None
        self.target_connector: BaseConnector = None
        self.multi_process_sync = MultiProcessSync(config)
        
    async def initialize(self) -> None:
        """初始化源和目标数据库连接"""
        try:
            # 打印配置内容
            logger.debug("当前配置:")
            logger.debug(f"batch_size: {self.config.batch_size}")
            logger.debug(f"max_concurrent_tasks: {self.config.max_concurrent_tasks}")
            logger.debug(f"verify_data: {self.config.verify_data}")
            logger.debug(f"retry_times: {self.config.retry_times}")
            logger.debug(f"retry_interval: {self.config.retry_interval}")
            
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
            
            logger.info(f"初始化完成，并发任务数: {self.config.max_concurrent_tasks}")
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
            
    async def sync_batch(self, table_mapping: TableMapping, batch_data: list, batch_num: int) -> int:
        """同步单个批次的数据"""
        retry_count = 0
        while retry_count < self.config.retry_times:
            batch_start_time = time.time()
            try:
                rows_written = await self.target_connector.write_data(table_mapping.target, batch_data)
                batch_end_time = time.time()
                batch_duration = batch_end_time - batch_start_time
                logger.info(f"批次 {batch_num} 同步完成: {rows_written} 行, "
                           f"耗时: {batch_duration:.2f}秒, "
                           f"速率: {rows_written/batch_duration:.2f} 行/秒")
                return rows_written
            except Exception as e:
                retry_count += 1
                if retry_count >= self.config.retry_times:
                    logger.error(f"批次 {batch_num} 同步失败，已重试 {retry_count} 次: {str(e)}")
                    raise
                logger.warning(f"批次 {batch_num} 同步失败，{self.config.retry_interval}秒后进行第 {retry_count + 1} 次重试")
                await asyncio.sleep(self.config.retry_interval)

    async def sync_table(self, table_mapping: TableMapping) -> bool:
        """同步单个表"""
        source_table = table_mapping.source
        target_table = table_mapping.target
        batch_size = self.config.batch_size
        
        try:
            # 如果配置了清空目标表
            if table_mapping.truncate:
                await self.truncate_table(target_table)
                logger.info(f"已清空目标表: {target_table}")
            
            # 开始同步
            table_start_time = time.time()
            total_rows = 0
            completed_batches = 0
            
            # 先读取所有批次数据
            all_batches = []
            async for batch_data in self.source_connector.read_data(source_table, batch_size):
                all_batches.append(batch_data)
            
            total_batches = len(all_batches)
            logger.info(f"总批次数: {total_batches}, 配置的并发数: {self.config.max_concurrent_tasks}")
            
            # 创建任务队列
            active_tasks = set()
            next_batch_index = 0
            
            async def process_next_batch():
                """处理下一个批次"""
                nonlocal next_batch_index, total_rows, completed_batches
                batch_num = next_batch_index + 1
                batch_data = all_batches[next_batch_index]
                next_batch_index += 1
                
                try:
                    rows = await self.sync_batch(table_mapping, batch_data, batch_num)
                    total_rows += rows
                    completed_batches += 1
                    return True
                except Exception as e:
                    logger.error(f"批次 {batch_num} 处理失败: {str(e)}")
                    return False
            
            # 初始启动最大并发数的任务
            for _ in range(min(self.config.max_concurrent_tasks, total_batches)):
                if next_batch_index < total_batches:
                    task = asyncio.create_task(process_next_batch())
                    active_tasks.add(task)
                    task.add_done_callback(active_tasks.discard)
            
            # 动态补充任务
            while active_tasks and next_batch_index < total_batches:
                # 等待任意一个任务完成
                done, pending = await asyncio.wait(
                    active_tasks,
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                # 检查完成的任务是否成功
                for completed_task in done:
                    try:
                        if not await completed_task:
                            # 任务失败，取消所有待处理任务
                            for task in pending:
                                task.cancel()
                            return False
                    except Exception as e:
                        logger.error(f"任务执行异常: {str(e)}")
                        for task in pending:
                            task.cancel()
                        return False
                
                # 为每个完成的任务创建新任务（如果还有未处理的批次）
                for _ in range(len(done)):
                    if next_batch_index < total_batches:
                        task = asyncio.create_task(process_next_batch())
                        active_tasks.add(task)
                        task.add_done_callback(active_tasks.discard)
            
            # 等待最后一组任务完成
            if active_tasks:
                try:
                    await asyncio.gather(*active_tasks)
                except Exception as e:
                    logger.error(f"最后一组任务处理失败: {str(e)}")
                    return False
            
            # 计算总耗时和性能指标
            table_end_time = time.time()
            total_duration = table_end_time - table_start_time
            avg_speed = total_rows / total_duration if total_duration > 0 else 0
            
            logger.success(f"表 {source_table} -> {target_table} 同步完成")
            logger.info(f"总记录数: {total_rows}, "
                       f"总耗时: {total_duration:.2f}秒, "
                       f"平均速率: {avg_speed:.2f} 行/秒, "
                       f"完成批次数: {completed_batches}/{total_batches}")
            
            # 如果配置了验证
            if self.config.verify_data and table_mapping.verify:
                source_count = await self.source_connector.get_row_count(source_table)
                target_count = await self.target_connector.get_row_count(target_table)
                
                if source_count != target_count:
                    logger.error(f"数据验证失败: 源表 {source_count} 行, 目标表 {target_count} 行")
                    return False
                    
                logger.success(f"数据验证成功: {source_count} 行")
            
            return True
            
        except Exception as e:
            logger.error(f"同步表 {source_table} 失败: {str(e)}")
            return False

    async def sync_all(self) -> bool:
        """同步所有配置的表"""
        try:
            logger.info("开始同步...")
            start_time = time.time()
            
            success = True
            for table_mapping in self.config.tables:
                if not await self.multi_process_sync.sync_table(table_mapping):
                    success = False
                    break
            
            end_time = time.time()
            total_duration = end_time - start_time
            
            if success:
                logger.success(f"所有表同步完成，总耗时: {total_duration:.2f}秒")
            else:
                logger.error(f"同步失败，总耗时: {total_duration:.2f}秒")
            
            return success
            
        except Exception as e:
            logger.error(f"同步过程发生错误: {str(e)}")
            return False
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