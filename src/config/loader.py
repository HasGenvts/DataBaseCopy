import json
from typing import Dict, List
from src.models.config import Config, DatabaseConfig, TableMapping, FieldMapping

def load_config(config_path: str) -> Config:
    """
    从JSON文件加载同步配置
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        Config对象
        
    Raises:
        FileNotFoundError: 配置文件不存在
        json.JSONDecodeError: JSON格式错误
        ValueError: 配置内容无效
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # 创建数据库配置
        source_config = DatabaseConfig(**data['source'])
        target_config = DatabaseConfig(**data['target'])
        
        # 创建表映射
        table_mappings = []
        for table in data['tables']:
            fields = [
                FieldMapping(**field) 
                for field in table['fields']
            ]
            mapping = TableMapping(
                source=table['source'],
                target=table['target'],
                fields=fields,
                truncate=table.get('truncate', False),
                verify=table.get('verify', True)
            )
            table_mappings.append(mapping)
            
        # 创建总配置
        config = Config(
            source=source_config,
            target=target_config,
            tables=table_mappings,
            batch_size=data.get('batch_size', 1000)
        )
        
        return config
        
    except FileNotFoundError:
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"配置文件JSON格式错误: {str(e)}")
    except KeyError as e:
        raise ValueError(f"配置文件缺少必要字段: {str(e)}")
    except Exception as e:
        raise ValueError(f"配置文件加载失败: {str(e)}") 