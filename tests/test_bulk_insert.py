import unittest
import mysql.connector
import random
from typing import List
import time
from loguru import logger

class TestBulkInsert(unittest.TestCase):
    def setUp(self):
        """测试前的设置，创建数据库连接和测试表"""
        self.conn = mysql.connector.connect(
            host="192.168.1.17",
            user="local",
            password="LLSctT7pBY75jmSb",  # 请修改为实际的密码
            database="LocalContainerManager"
        )
        self.cursor = self.conn.cursor()
        
        # 创建测试表
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS cn_test (
                name VARCHAR(64) NOT NULL,
                INDEX idx_name (name)
            )
        """)
        self.conn.commit()

    def generate_name(self) -> str:
        """生成随机中文名"""
        first_names = ['张', '王', '李', '赵', '刘', '陈', '杨', '黄', '周', '吴',
                      '郑', '孙', '马', '朱', '胡', '林', '郭', '何', '高', '罗']
        last_names = ['伟', '芳', '娜', '秀英', '敏', '静', '丽', '强', '磊', '洋',
                     '艳', '勇', '军', '杰', '涛', '超', '明', '波', '辉', '刚']
        return random.choice(first_names) + random.choice(last_names)

    def generate_test_data(self, batch_size: int) -> List[tuple]:
        """生成测试数据"""
        return [(self.generate_name(),) for _ in range(batch_size)]

    def test_bulk_insert(self):
        """测试批量插入50万条数据"""
        total_records = 500000
        batch_size = 5000
        total_batches = total_records // batch_size
        
        logger.info(f"开始批量插入测试，总记录数: {total_records}, 批次大小: {batch_size}")
        start_time = time.time()
        
        for batch_num in range(total_batches):
            batch_start_time = time.time()
            
            # 生成当前批次的测试数据
            test_data = self.generate_test_data(batch_size)
            
            # 批量插入数据
            insert_query = "INSERT INTO cn_test (name) VALUES (%s)"
            self.cursor.executemany(insert_query, test_data)
            self.conn.commit()
            
            batch_end_time = time.time()
            batch_duration = batch_end_time - batch_start_time
            progress = (batch_num + 1) / total_batches * 100
            
            logger.info(f"批次 {batch_num + 1}/{total_batches} 完成 "
                       f"({progress:.2f}%), 耗时: {batch_duration:.2f}秒, "
                       f"速率: {batch_size/batch_duration:.2f} 记录/秒")
        
        end_time = time.time()
        total_duration = end_time - start_time
        average_speed = total_records / total_duration
        
        logger.info(f"测试完成！总耗时: {total_duration:.2f}秒, "
                   f"平均速率: {average_speed:.2f} 记录/秒")
        
        # 验证插入的记录数
        self.cursor.execute("SELECT COUNT(*) FROM cn_test")
        count = self.cursor.fetchone()[0]
        self.assertEqual(count, total_records)

    def tearDown(self):
        self.cursor.close()
        self.conn.close()

if __name__ == '__main__':
    unittest.main() 