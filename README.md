# DataBaseCopy

[![GitHub stars](https://img.shields.io/github/stars/HasGenvts/DataBaseCopy.svg)](https://github.com/HasGenvts/DataBaseCopy/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/HasGenvts/DataBaseCopy.svg)](https://github.com/HasGenvts/DataBaseCopy/network)
[![GitHub issues](https://img.shields.io/github/issues/HasGenvts/DataBaseCopy.svg)](https://github.com/HasGenvts/DataBaseCopy/issues)
[![GitHub license](https://img.shields.io/github/license/HasGenvts/DataBaseCopy.svg)](https://github.com/HasGenvts/DataBaseCopy/blob/main/LICENSE)

一个高性能的数据库表同步工具，支持多进程并行处理，可以快速地在不同数据库之间同步数据。

![Stargazers over time](https://starchart.cc/HasGenvts/DataBaseCopy.svg)

## ✨ 特性

- 🚀 **高性能**：采用多进程并行处理，显著提升同步速度
- 🔄 **增量同步**：支持增量数据同步，避免重复传输
- 🛡 **数据验证**：内置数据验证功能，确保同步数据的完整性
- 🎯 **断点续传**：支持同步任务断点续传，提高容错性
- 📊 **实时监控**：详细的进度展示和性能指标统计
- 🔌 **多数据库支持**：支持 MySQL、SQL Server 等多种数据库

## 🎯 性能指标

- 单表百万级数据同步时间：< 5分钟
- 支持最大并发数：50
- 内存占用：< 2GB
- CPU 使用率：< 70%

## 🚀 快速开始

### 安装

```bash
git clone https://github.com/HasGenvts/DataBaseCopy.git
cd DataBaseCopy
pip install -r requirements.txt
```

### 配置

创建配置文件 `configM2P.json`：

```json
{
  "source": {
    "type": "mysql",
    "host": "source_host",
    "port": 3306,
    "username": "root",
    "password": "password",
    "database": "db_name"
  },
  "target": {
    "type": "postgresql",
    "host": "target_host",
    "port": 5432,
    "username": "postgres",
    "password": "password",
    "database": "db_name"
  },
  "tables": [
    {
      "source": "source_table",
      "target": "target_table",
      "fields": [
        {"source": "id", "target": "id"},
        {"source": "name", "target": "name"}
      ],
      "truncate": true,
      "verify": true
    }
  ],
  "batch_size": 10000,
  "max_concurrent_tasks": 10,
  "verify_data": true,
  "retry_times": 3,
  "retry_interval": 1
}
```

### 运行

```bash
python main.py config=configM2P.json
```

## 📊 监控输出

```
开始同步: source_table -> target_table
总批次数: 100, 并发数: 10

进程 01 - 批次 001: 10000 行, 耗时:   2.50秒, 速率: 4000.00 行/秒
进程 02 - 批次 002:  8000 行, 耗时:   2.10秒, 速率: 3809.52 行/秒
...

同步完成统计:
总记录数: 1,000,000
总耗时: 250.50秒
平均速率: 3,990.02 行/秒
完成批次: 100/100

数据验证成功: 1,000,000 行
```

## 🔧 配置说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| batch_size | 每批处理的数据量 | 10000 |
| max_concurrent_tasks | 最大并发数 | 10 |
| verify_data | 是否验证数据 | true |
| retry_times | 失败重试次数 | 3 |
| retry_interval | 重试间隔(秒) | 1 |

## 🤝 贡献指南

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交改动 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 提交 Pull Request

## 📝 开源协议

本项目采用 MIT 协议 - 详见 [LICENSE](LICENSE) 文件

## 🌟 Star 历史

[![Star History Chart](https://api.star-history.com/svg?repos=HasGenvts/DataBaseCopy&type=Date)](https://star-history.com/#HasGenvts/DataBaseCopy&Date)

## 📧 联系方式

如有问题或建议，欢迎提交 [Issue](https://github.com/HasGenvts/DataBaseCopy/issues)。
