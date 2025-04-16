# DataBaseCopy

一个强大的数据库同步工具，支持在不同类型数据库之间进行数据迁移和同步。

## 功能特点

- 支持多种数据库: MySQL、PostgreSQL、SQL Server
- 灵活的表和字段映射配置
- 批量处理以提高性能
- 详细的日志记录
- 数据验证功能
- 支持增量同步

## 安装要求

- Python 3.8+
- 相关数据库驱动:
  - MySQL: `mysql-connector-python`
  - PostgreSQL: `psycopg2-binary`
  - SQL Server: `pyodbc`

## 安装步骤

```bash
# 克隆仓库
git clone https://github.com/yourusername/DataBaseCopy.git
cd DataBaseCopy

# 安装依赖
pip install -r requirements.txt
```

## 使用方法

1. 创建配置文件 (例如 `config.json`):

```json
{
    "source": {
        "host": "source-host",
        "port": 3306,
        "username": "user",
        "password": "password",
        "database": "db_name"
    },
    "target": {
        "host": "target-host",
        "port": 5432,
        "username": "user",
        "password": "password",
        "database": "db_name",
        "schema": "public"
    },
    "tables": [
        {
            "source": "source_table",
            "target": "target_table",
            "fields": [
                {
                    "source": "id",
                    "target": "id"
                }
            ]
        }
    ],
    "batch_size": 5000
}
```

2. 运行同步程序:

```bash
python main.py config=path/to/config.json
```

## 配置文件说明

### 数据库配置

- `source`: 源数据库配置
- `target`: 目标数据库配置
- `tables`: 表映射配置
- `batch_size`: 批处理大小

### 支持的数据库参数

#### MySQL
- host
- port
- username
- password
- database

#### PostgreSQL
- host
- port
- username
- password
- database
- schema
- sslmode

#### SQL Server
- host
- port
- username
- password
- database
- driver
- trust_server_certificate

## 示例配置

项目包含多个示例配置文件:

- `examples/mysql_to_postgresql.json`: MySQL 到 PostgreSQL 的迁移
- `examples/postgresql_to_mysql.json`: PostgreSQL 到 MySQL 的迁移
- `examples/sqlserver_to_postgresql.json`: SQL Server 到 PostgreSQL 的迁移
- `examples/local_dev.json`: 本地开发测试配置

## 日志

- 日志文件位置: `sync.log`
- 支持日志轮转（超过500MB自动创建新文件）
- 同时在控制台显示操作信息

## 贡献

欢迎提交 Issue 和 Pull Request！

## 许可证

MIT License
