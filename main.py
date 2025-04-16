import asyncio
import json
import sys
from pathlib import Path
from loguru import logger
from src.models.config import SyncConfig, DatabaseConfig, TableMapping, FieldMapping

# 移除默认的处理器
logger.remove()

# 添加文件处理器
logger.add(
    "sync.log",
    rotation="500 MB",
    level="INFO",
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
)

# 添加控制台处理器
logger.add(
    sys.stdout,
    level="DEBUG",
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <level>{message}</level>"
)

def parse_args() -> str:
    """解析命令行参数"""
    config_path = None
    for arg in sys.argv[1:]:
        if arg.startswith("config="):
            # 去除可能存在的引号
            config_path = arg.split("=", 1)[1].strip("'\"")
            break
    
    if not config_path:
        raise ValueError("Missing required argument: config=<path_to_config_file>")
    
    logger.debug(f"Parsed config path: {config_path}")
    return config_path

async def main():
    try:
        # 解析命令行参数获取配置文件路径
        config_path = parse_args()
        config_file = Path(config_path)
        
        logger.debug(f"Checking file existence: {config_file.absolute()}")
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_file.absolute()}")
            
        logger.info(f"Loading configuration from: {config_path}")
        with open(config_file, "r", encoding="utf-8") as f:
            config_data = json.load(f)
        
        # 导入配置类
        from src.models.config import SyncConfig, DatabaseConfig, TableMapping, FieldMapping
        
        # 创建数据库配置对象
        source_config = DatabaseConfig(**config_data["source"])
        target_config = DatabaseConfig(**config_data["target"])
        
        # 创建表映射对象
        table_mappings = []
        for table in config_data["tables"]:
            fields = [FieldMapping(**field) for field in table["fields"]]
            mapping = TableMapping(
                source=table["source"],
                target=table["target"],
                fields=fields,
                truncate=table.get("truncate", False),
                verify=table.get("verify", True)
            )
            table_mappings.append(mapping)
            
        # 创建同步配置对象
        config = SyncConfig(
            source=source_config,
            target=target_config,
            tables=table_mappings,
            batch_size=config_data.get("batch_size", 1000)
        )
        
        # 创建同步服务
        from src.services.sync import SyncService
        sync_service = SyncService(config)
        
        # 执行同步
        await sync_service.sync_all()
        
    except Exception as e:
        logger.error(f"Sync failed: {str(e)}")
        raise
        
if __name__ == "__main__":
    asyncio.run(main())