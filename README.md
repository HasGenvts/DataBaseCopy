# 数据库同步工具 (Database Sync Tool)

这是一个用于在不同数据库之间同步数据的Python工具。支持MySQL、PostgreSQL和SQL Server之间的数据同步。

## 功能特性

- 支持多种数据库
  - MySQL
  - PostgreSQL
  - SQL Server
- 支持任意数据库间的组合同步
- 灵活的表名和字段名映射
- 批量数据处理
- 异步处理提高性能
- 详细的日志记录
- 表结构兼容性检查
- SSL安全连接支持

## 系统要求

- Python 3.8+
- 对应数据库的客户端驱动
  - MySQL: 无需额外安装
  - PostgreSQL: 需要安装PostgreSQL客户端
  - SQL Server: 需要安装ODBC驱动

## 安装

1. 克隆代码库：
```bash
git clone <repository_url>
cd database-sync
```

2. 安装依赖：
```bash
pip install -r requirements.txt
```

## 配置说明

在项目根目录创建 `config.json` 文件，根据需要配置源数据库和目标数据库信息。

### 基础配置结构：
```json
{
    "source": {
        "type": "mysql",
        "host": "localhost",
        "port": 3306,
        "username": "root",
        "password": "password123",
        "database": "source_db"
    },
    "target": {
        "type": "postgresql",
        "host": "localhost",
        "port": 5432,
        "username": "postgres",
        "password": "password123",
        "database": "target_db"
    },
    "table_mappings": {
        "users": "app_users",
        "orders": "app_orders"
    },
    "field_mappings": {
        "users": {
            "user_id": "id",
            "user_name": "username"
        }
    },
    "batch_size": 1000
}
```

### 数据库特有配置

1. SQL Server 特有配置：
```json
{
    "type": "sqlserver",
    "host": "localhost",
    "port": 1433,
    "username": "sa",
    "password": "password123",
    "database": "source_db",
    "driver": "ODBC Driver 17 for SQL Server",
    "trust_server_certificate": true
}
```

2. PostgreSQL 特有配置：
```json
{
    "type": "postgresql",
    "host": "localhost",
    "port": 5432,
    "username": "postgres",
    "password": "password123",
    "database": "target_db",
    "pg_schema": "public",
    "sslmode": "verify-full"
}
```

## 使用示例

项目提供了多个配置示例，位于 `examples` 目录：

1. MySQL到PostgreSQL同步 (`mysql_to_postgresql.json`)
   - 适用于电商系统数据迁移
   - 包含客户、订单等表的映射

2. PostgreSQL到MySQL同步 (`postgresql_to_mysql.json`)
   - 适用于应用数据备份
   - 包含用户、文章等表的映射

3. SQL Server到PostgreSQL同步 (`sqlserver_to_postgresql.json`)
   - 适用于企业级数据迁移
   - 包含销售、员工等表的映射

4. 本地开发测试 (`local_dev.json`)
   - 适用于本地开发环境
   - 包含基础测试表配置

### 使用方法

1. 选择合适的配置示例并复制到项目根目录：
```bash
cp examples/mysql_to_postgresql.json config.json
```

2. 修改配置文件中的连接信息

3. 运行同步程序：
```bash
python src/main.py
```

## 注意事项

1. 数据安全
   - 在进行数据同步前务必备份目标数据库
   - 建议先在测试环境验证配置
   - 生产环境中使用加密方式保存数据库密码

2. 性能优化
   - 根据数据量调整 batch_size
   - 大表同步建议在低峰期进行
   - 可以通过修改批处理大小优化性能

3. 兼容性
   - 确保目标表结构与源表兼容
   - 注意不同数据库的字段类型映射
   - 某些特殊字段可能需要手动转换

4. 环境要求
   - SQL Server需要安装对应版本的ODBC驱动
   - PostgreSQL建议使用psycopg2-binary
   - 确保数据库用户有足够的权限

## 故障排除

1. 连接问题
   - 检查网络连接和防火墙设置
   - 验证数据库用户权限
   - 确认数据库服务是否正常运行

2. 同步错误
   - 查看日志文件了解详细错误信息
   - 检查表结构兼容性
   - 验证字段映射配置

## 开发扩展

要添加新的数据库支持：

1. 在 `src/connectors` 目录下创建新的连接器类
2. 继承 `BaseConnector` 类并实现所有抽象方法
3. 在 `ConnectorFactory` 中注册新的连接器
4. 更新配置模型中的数据库类型定义

## 日志说明

- 日志文件：`sync.log`
- 默认日志级别：INFO
- 日志轮转：500MB
- 包含详细的同步进度和错误信息

## 许可证

MIT License

## 贡献指南

欢迎提交Issue和Pull Request来帮助改进这个项目。

## 联系方式

如有问题或建议，请通过Issue与我们联系。
