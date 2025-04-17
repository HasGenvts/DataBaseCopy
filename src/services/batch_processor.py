import time
import traceback
import asyncio
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from typing import List, Dict, Any, Tuple
from multiprocessing import Process, Queue
from queue import Empty
from loguru import logger
from src.models.config import TableMapping, SyncConfig, DatabaseConfig
from src.connectors.factory import ConnectorFactory

def run_async(coro):
    """在子进程中运行异步代码"""
    return asyncio.get_event_loop().run_until_complete(coro)

def serialize_config(config: SyncConfig) -> Dict[str, Any]:
    """序列化配置对象为字典"""
    return {
        'source': {
            'type': config.source.type,
            'host': config.source.host,
            'port': config.source.port,
            'username': config.source.username,
            'password': config.source.password,
            'database': config.source.database,
            'driver': config.source.driver,
            'schema': config.source.schema,
            'sslmode': config.source.sslmode,
            'trust_server_certificate': config.source.trust_server_certificate
        },
        'target': {
            'type': config.target.type,
            'host': config.target.host,
            'port': config.target.port,
            'username': config.target.username,
            'password': config.target.password,
            'database': config.target.database,
            'driver': config.target.driver,
            'schema': config.target.schema,
            'sslmode': config.target.sslmode,
            'trust_server_certificate': config.target.trust_server_certificate
        },
        'batch_size': config.batch_size,
        'max_concurrent_tasks': config.max_concurrent_tasks,
        'verify_data': config.verify_data,
        'retry_times': config.retry_times,
        'retry_interval': config.retry_interval
    }

def serialize_table_mapping(mapping: TableMapping) -> Dict[str, Any]:
    """序列化表映射对象为字典"""
    return {
        'source': mapping.source,
        'target': mapping.target,
        'fields': [{'source': f.source, 'target': f.target} for f in mapping.fields],
        'truncate': mapping.truncate,
        'verify': mapping.verify
    }

def deserialize_config(data: Dict[str, Any]) -> SyncConfig:
    """从字典反序列化配置对象"""
    source_config = DatabaseConfig(**data['source'])
    target_config = DatabaseConfig(**data['target'])
    return SyncConfig(
        source=source_config,
        target=target_config,
        tables=[],  # 不需要完整的表配置
        batch_size=data['batch_size'],
        max_concurrent_tasks=data['max_concurrent_tasks'],
        verify_data=data['verify_data'],
        retry_times=data['retry_times'],
        retry_interval=data['retry_interval']
    )

def deserialize_table_mapping(data: Dict[str, Any]) -> TableMapping:
    """从字典反序列化表映射对象"""
    from src.models.config import FieldMapping
    fields = [FieldMapping(**f) for f in data['fields']]
    return TableMapping(
        source=data['source'],
        target=data['target'],
        fields=fields,
        truncate=data['truncate'],
        verify=data['verify']
    )

def process_worker(config_dict: Dict[str, Any], 
                  table_mapping_dict: Dict[str, Any],
                  process_id: int,
                  batch_data: List[Dict[str, Any]],
                  batch_num: int) -> Tuple[int, bool, int]:
    """工作进程的入口函数"""
    try:
        # 反序列化配置
        config = deserialize_config(config_dict)
        table_mapping = deserialize_table_mapping(table_mapping_dict)
        
        processor = BatchProcessor(
            config=config,
            table_mapping=table_mapping,
            process_id=process_id
        )
        
        # 初始化处理器
        if not run_async(processor.initialize()):
            return batch_num, False, 0
        
        # 处理批次
        success, rows = run_async(processor.process_batch(batch_data, batch_num))
        
        # 清理资源
        run_async(processor.cleanup())
        
        return batch_num, success, rows
        
    except Exception as e:
        logger.error(f"进程 {process_id} 发生异常: {str(e)}\n{traceback.format_exc()}")
        return batch_num, False, 0

class BatchProcessor:
    """批次处理器，在子进程中执行"""
    
    def __init__(self, 
                 config: SyncConfig,
                 table_mapping: TableMapping,
                 process_id: int):
        self.config = config
        self.table_mapping = table_mapping
        self.process_id = process_id
        self.source_connector = None
        self.target_connector = None
        
    async def initialize(self):
        """初始化数据库连接"""
        try:
            # 创建源和目标数据库连接
            self.source_connector = ConnectorFactory.get_connector(
                self.config.source.type,
                self.config.source
            )
            self.target_connector = ConnectorFactory.get_connector(
                self.config.target.type,
                self.config.target
            )
            
            # 建立连接
            await self.source_connector.connect()
            await self.target_connector.connect()
            
            logger.info(f"进程 {self.process_id} 初始化完成")
            return True
        except Exception as e:
            logger.error(f"进程 {self.process_id} 初始化失败: {str(e)}")
            return False
            
    async def cleanup(self):
        """清理资源"""
        try:
            if self.source_connector:
                await self.source_connector.disconnect()
            if self.target_connector:
                await self.target_connector.disconnect()
        except Exception as e:
            logger.error(f"进程 {self.process_id} 清理资源失败: {str(e)}")
    
    async def process_batch(self, batch_data: List[Dict[str, Any]], batch_num: int) -> Tuple[bool, int]:
        """处理单个批次的数据"""
        retry_count = 0
        while retry_count < self.config.retry_times:
            batch_start_time = time.time()
            try:
                # 写入数据
                rows_written = await self.target_connector.write_data(
                    self.table_mapping.target, 
                    batch_data
                )
                
                # 计算性能指标
                batch_end_time = time.time()
                batch_duration = batch_end_time - batch_start_time
                rate = rows_written / batch_duration if batch_duration > 0 else 0
                
                logger.info(
                    f"进程 {self.process_id} - 批次 {batch_num} 完成: "
                    f"{rows_written} 行, 耗时: {batch_duration:.2f}秒, "
                    f"速率: {rate:.2f} 行/秒"
                )
                
                return True, rows_written
                
            except Exception as e:
                retry_count += 1
                if retry_count >= self.config.retry_times:
                    logger.error(
                        f"进程 {self.process_id} - 批次 {batch_num} 失败, "
                        f"已重试 {retry_count} 次: {str(e)}\n"
                        f"{traceback.format_exc()}"
                    )
                    return False, 0
                    
                logger.warning(
                    f"进程 {self.process_id} - 批次 {batch_num} 失败, "
                    f"{self.config.retry_interval}秒后进行第 {retry_count + 1} 次重试"
                )
                await asyncio.sleep(self.config.retry_interval)
        
        return False, 0

class MultiProcessSync:
    """多进程同步管理器"""
    
    def __init__(self, config: SyncConfig):
        self.config = config
        self.current_table_mapping = None
        
    async def sync_table(self, table_mapping: TableMapping) -> bool:
        """同步单个表"""
        self.current_table_mapping = table_mapping
        source_table = table_mapping.source
        target_table = table_mapping.target
        batch_size = self.config.batch_size
        
        try:
            # 创建临时连接读取数据
            source_connector = ConnectorFactory.get_connector(
                self.config.source.type,
                self.config.source
            )
            await source_connector.connect()
            
            # 如果配置了清空目标表
            if table_mapping.truncate:
                target_connector = ConnectorFactory.get_connector(
                    self.config.target.type,
                    self.config.target
                )
                await target_connector.connect()
                await target_connector.execute(f"TRUNCATE TABLE {target_table}")
                await target_connector.disconnect()
                logger.info(f"已清空目标表: {target_table}")
            
            # 开始同步
            table_start_time = time.time()
            
            # 读取所有批次数据
            all_batches = []
            async for batch_data in source_connector.read_data(source_table, batch_size):
                all_batches.append(batch_data)
            
            # 关闭临时连接
            await source_connector.disconnect()
            
            total_batches = len(all_batches)
            logger.info(f"总批次数: {total_batches}, 配置的并发数: {self.config.max_concurrent_tasks}")
            
            # 序列化配置
            config_dict = serialize_config(self.config)
            table_mapping_dict = serialize_table_mapping(table_mapping)
            
            # 创建进程池
            with ProcessPoolExecutor(max_workers=self.config.max_concurrent_tasks) as executor:
                # 提交所有任务
                futures = []
                for batch_num, batch_data in enumerate(all_batches, 1):
                    future = executor.submit(
                        process_worker,
                        config_dict,
                        table_mapping_dict,
                        batch_num % self.config.max_concurrent_tasks,
                        batch_data,
                        batch_num
                    )
                    futures.append(future)
                
                # 等待所有任务完成
                total_rows = 0
                failed_batches = []
                
                for future in futures:
                    try:
                        batch_num, success, rows = future.result()
                        if success:
                            total_rows += rows
                        else:
                            failed_batches.append(batch_num)
                    except Exception as e:
                        logger.error(f"任务执行失败: {str(e)}")
                        return False
                
                # 计算总耗时和性能指标
                table_end_time = time.time()
                total_duration = table_end_time - table_start_time
                avg_speed = total_rows / total_duration if total_duration > 0 else 0
                
                logger.success(f"表 {source_table} -> {target_table} 同步完成")
                logger.info(
                    f"总记录数: {total_rows}, 总耗时: {total_duration:.2f}秒, "
                    f"平均速率: {avg_speed:.2f} 行/秒, "
                    f"完成批次数: {total_batches - len(failed_batches)}/{total_batches}"
                )
                
                if failed_batches:
                    logger.warning(f"失败批次: {failed_batches}")
                    return False
                
                # 如果配置了验证
                if self.config.verify_data and table_mapping.verify:
                    # 创建临时连接进行验证
                    source_connector = ConnectorFactory.get_connector(
                        self.config.source.type,
                        self.config.source
                    )
                    target_connector = ConnectorFactory.get_connector(
                        self.config.target.type,
                        self.config.target
                    )
                    
                    await source_connector.connect()
                    await target_connector.connect()
                    
                    source_count = await source_connector.get_row_count(source_table)
                    target_count = await target_connector.get_row_count(target_table)
                    
                    await source_connector.disconnect()
                    await target_connector.disconnect()
                    
                    if source_count != target_count:
                        logger.error(f"数据验证失败: 源表 {source_count} 行, 目标表 {target_count} 行")
                        return False
                        
                    logger.success(f"数据验证成功: {source_count} 行")
                
                return True
                
        except Exception as e:
            logger.error(f"同步表 {source_table} 失败: {str(e)}\n{traceback.format_exc()}")
            return False 