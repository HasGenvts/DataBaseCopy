import mysql.connector
import psycopg2
import pyodbc
from typing import Any
from src.models.config import DatabaseConfig

def get_connection(config: DatabaseConfig) -> Any:
    """
    根据配置创建数据库连接
    """
    if config.type.lower() == "mysql":
        return mysql.connector.connect(
            host=config.host,
            port=config.port,
            user=config.username,
            password=config.password,
            database=config.database
        )
    
    elif config.type.lower() == "postgresql":
        conn_params = {
            "host": config.host,
            "port": config.port,
            "user": config.username,
            "password": config.password,
            "database": config.database
        }
        
        if hasattr(config, "schema"):
            conn_params["options"] = f"-c search_path={config.schema}"
        
        if hasattr(config, "sslmode"):
            conn_params["sslmode"] = config.sslmode
            
        return psycopg2.connect(**conn_params)
    
    elif config.type.lower() == "sqlserver":
        conn_str = (
            f"DRIVER={{{config.driver}}};"
            f"SERVER={config.host},{config.port};"
            f"DATABASE={config.database};"
            f"UID={config.username};"
            f"PWD={config.password}"
        )
        
        if hasattr(config, "trust_server_certificate"):
            conn_str += f";TrustServerCertificate={'yes' if config.trust_server_certificate else 'no'}"
            
        return pyodbc.connect(conn_str)
    
    else:
        raise ValueError(f"Unsupported database type: {config.type}") 