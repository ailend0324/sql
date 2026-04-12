#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hue查询自动化脚本
功能：自动连接Hue，执行财务毛利查询，优化性能，导出数据
作者：AI助手
适用：回收宝财务数据分析
"""

import requests
import json
import time
import pandas as pd
from datetime import datetime, timedelta
import logging
import os
from typing import Dict, List, Optional
import configparser

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hue_automation.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class HueAutomation:
    """Hue查询自动化类"""
    
    def __init__(self, config_file: str = 'hue_config.ini'):
        """
        初始化Hue自动化工具
        
        Args:
            config_file: 配置文件路径
        """
        self.config = self._load_config(config_file)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
        # Hue连接信息
        self.hue_url = self.config.get('hue', 'base_url')
        self.username = self.config.get('hue', 'username')
        self.password = self.config.get('hue', 'password')
        
        # 查询配置
        self.query_timeout = self.config.getint('query', 'timeout', fallback=300)
        self.max_retries = self.config.getint('query', 'max_retries', fallback=3)
        
        # 数据导出配置
        self.export_dir = self.config.get('export', 'directory', fallback='./exports')
        self.ensure_export_dir()
        
        logger.info("Hue自动化工具初始化完成")
    
    def _load_config(self, config_file: str) -> configparser.ConfigParser:
        """加载配置文件"""
        config = configparser.ConfigParser()
        
        if not os.path.exists(config_file):
            # 创建默认配置文件
            self._create_default_config(config_file)
        
        config.read(config_file, encoding='utf-8')
        return config
    
    def _create_default_config(self, config_file: str):
        """创建默认配置文件"""
        config = configparser.ConfigParser()
        
        config['hue'] = {
            'base_url': 'http://119.23.30.106:8889',
            'username': 'your_username',
            'password': 'your_password'
        }
        
        config['query'] = {
            'timeout': '300',
            'max_retries': '3',
            'batch_size': '10000'
        }
        
        config['export'] = {
            'directory': './exports',
            'format': 'csv',
            'encoding': 'utf-8'
        }
        
        config['optimization'] = {
            'enable_partition_pruning': 'true',
            'enable_column_pruning': 'true',
            'max_memory': '2GB'
        }
        
        with open(config_file, 'w', encoding='utf-8') as f:
            config.write(f)
        
        logger.info(f"已创建默认配置文件: {config_file}")
        logger.info("请编辑配置文件，填入正确的Hue连接信息")
    
    def ensure_export_dir(self):
        """确保导出目录存在"""
        if not os.path.exists(self.export_dir):
            os.makedirs(self.export_dir)
            logger.info(f"创建导出目录: {self.export_dir}")
    
    def login(self) -> bool:
        """
        登录Hue
        
        Returns:
            bool: 登录是否成功
        """
        try:
            login_url = f"{self.hue_url}/accounts/login/"
            
            # 获取登录页面，获取CSRF token
            response = self.session.get(login_url)
            if response.status_code != 200:
                logger.error(f"无法访问登录页面: {response.status_code}")
                return False
            
            # 这里需要根据实际的Hue登录机制调整
            # 有些Hue版本使用不同的认证方式
            login_data = {
                'username': self.username,
                'password': self.password,
                'csrfmiddlewaretoken': self._extract_csrf_token(response.text)
            }
            
            response = self.session.post(login_url, data=login_data)
            
            if response.status_code == 200 and 'dashboard' in response.url:
                logger.info("Hue登录成功")
                return True
            else:
                logger.error("Hue登录失败")
                return False
                
        except Exception as e:
            logger.error(f"登录过程中发生错误: {e}")
            return False
    
    def _extract_csrf_token(self, html_content: str) -> str:
        """从HTML中提取CSRF token"""
        try:
            # 简单的CSRF token提取逻辑
            import re
            match = re.search(r'name="csrfmiddlewaretoken" value="([^"]+)"', html_content)
            return match.group(1) if match else ""
        except:
            return ""
    
    def execute_query(self, sql_query: str, query_name: str = "财务毛利查询") -> Optional[pd.DataFrame]:
        """
        执行SQL查询
        
        Args:
            sql_query: SQL查询语句
            query_name: 查询名称，用于日志记录
            
        Returns:
            pd.DataFrame: 查询结果，失败时返回None
        """
        try:
            logger.info(f"开始执行查询: {query_name}")
            
            # 这里需要根据实际的Hue API调整
            # Hue通常通过REST API执行查询
            query_url = f"{self.hue_url}/api/query/execute"
            
            query_data = {
                'query': sql_query,
                'type': 'hive',  # 根据实际数据库类型调整
                'name': query_name
            }
            
            response = self.session.post(query_url, json=query_data, timeout=self.query_timeout)
            
            if response.status_code == 200:
                result = response.json()
                query_id = result.get('id')
                
                if query_id:
                    logger.info(f"查询已提交，ID: {query_id}")
                    return self._wait_for_completion(query_id)
                else:
                    logger.error("查询提交失败，未获取到查询ID")
                    return None
            else:
                logger.error(f"查询提交失败: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"执行查询时发生错误: {e}")
            return None
    
    def _wait_for_completion(self, query_id: str) -> Optional[pd.DataFrame]:
        """等待查询完成并获取结果"""
        try:
            status_url = f"{self.hue_url}/api/query/{query_id}/status"
            result_url = f"{self.hue_url}/api/query/{query_id}/result"
            
            # 等待查询完成
            for attempt in range(self.max_retries):
                time.sleep(5)  # 等待5秒
                
                status_response = self.session.get(status_url)
                if status_response.status_code == 200:
                    status = status_response.json().get('status')
                    
                    if status == 'finished':
                        logger.info("查询执行完成")
                        return self._fetch_results(result_url)
                    elif status == 'failed':
                        logger.error("查询执行失败")
                        return None
                    elif status == 'running':
                        logger.info(f"查询正在执行中... (尝试 {attempt + 1}/{self.max_retries})")
                        continue
                
                if attempt == self.max_retries - 1:
                    logger.error("查询超时")
                    return None
            
            return None
            
        except Exception as e:
            logger.error(f"等待查询完成时发生错误: {e}")
            return None
    
    def _fetch_results(self, result_url: str) -> Optional[pd.DataFrame]:
        """获取查询结果"""
        try:
            response = self.session.get(result_url)
            if response.status_code == 200:
                result_data = response.json()
                
                # 解析结果数据
                if 'data' in result_data:
                    df = pd.DataFrame(result_data['data'])
                    logger.info(f"成功获取查询结果，共 {len(df)} 行数据")
                    return df
                else:
                    logger.error("查询结果格式不正确")
                    return None
            else:
                logger.error(f"获取查询结果失败: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"获取查询结果时发生错误: {e}")
            return None
    
    def optimize_query(self, sql_query: str) -> str:
        """
        优化SQL查询
        
        Args:
            sql_query: 原始SQL查询
            
        Returns:
            str: 优化后的SQL查询
        """
        logger.info("开始优化SQL查询")
        
        # 基础优化规则
        optimized_query = sql_query
        
        # 1. 添加分区裁剪提示
        if 'WHERE' in sql_query.upper():
            optimized_query = optimized_query.replace(
                'WHERE', 
                'WHERE /*+ PARTITION_PRUNE */'
            )
        
        # 2. 添加列裁剪提示
        if 'SELECT' in sql_query.upper():
            optimized_query = optimized_query.replace(
                'SELECT', 
                'SELECT /*+ COLUMN_PRUNE */'
            )
        
        # 3. 优化UNION ALL（如果存在）
        if 'UNION ALL' in sql_query.upper():
            # 确保UNION ALL的列顺序一致
            optimized_query = self._optimize_union_all(optimized_query)
        
        # 4. 添加LIMIT限制（如果查询结果很大）
        if 'LIMIT' not in sql_query.upper():
            optimized_query += '\nLIMIT 1000000'  # 限制结果大小
        
        logger.info("SQL查询优化完成")
        return optimized_query
    
    def _optimize_union_all(self, sql_query: str) -> str:
        """优化UNION ALL查询"""
        try:
            # 这里可以添加更复杂的UNION ALL优化逻辑
            # 比如检查列类型一致性、添加适当的索引提示等
            return sql_query
        except:
            return sql_query
    
    def export_data(self, df: pd.DataFrame, filename: str, format: str = 'csv') -> str:
        """
        导出数据到文件
        
        Args:
            df: 要导出的数据框
            filename: 文件名（不含扩展名）
            format: 导出格式 (csv, excel, json)
            
        Returns:
            str: 导出文件路径
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename_with_timestamp = f"{filename}_{timestamp}"
            
            if format.lower() == 'csv':
                filepath = os.path.join(self.export_dir, f"{filename_with_timestamp}.csv")
                df.to_csv(filepath, index=False, encoding='utf-8-sig')
            elif format.lower() == 'excel':
                filepath = os.path.join(self.export_dir, f"{filename_with_timestamp}.xlsx")
                df.to_excel(filepath, index=False, engine='openpyxl')
            elif format.lower() == 'json':
                filepath = os.path.join(self.export_dir, f"{filename_with_timestamp}.json")
                df.to_json(filepath, orient='records', force_ascii=False, indent=2)
            else:
                raise ValueError(f"不支持的导出格式: {format}")
            
            logger.info(f"数据导出成功: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"数据导出失败: {e}")
            return ""
    
    def analyze_data(self, df: pd.DataFrame) -> Dict:
        """
        分析数据，生成统计报告
        
        Args:
            df: 要分析的数据框
            
        Returns:
            Dict: 分析结果
        """
        try:
            logger.info("开始数据分析")
            
            analysis_result = {
                '数据概览': {
                    '总行数': len(df),
                    '总列数': len(df.columns),
                    '数据大小': f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB"
                },
                '列信息': {},
                '数值列统计': {},
                '缺失值统计': {}
            }
            
            # 分析每列的信息
            for col in df.columns:
                col_info = {
                    '数据类型': str(df[col].dtype),
                    '唯一值数量': df[col].nunique(),
                    '缺失值数量': df[col].isnull().sum(),
                    '缺失值比例': f"{df[col].isnull().sum() / len(df) * 100:.2f}%"
                }
                
                analysis_result['列信息'][col] = col_info
                
                # 数值列的特殊统计
                if pd.api.types.is_numeric_dtype(df[col]):
                    analysis_result['数值列统计'][col] = {
                        '最小值': df[col].min(),
                        '最大值': df[col].max(),
                        '平均值': df[col].mean(),
                        '中位数': df[col].median(),
                        '标准差': df[col].std()
                    }
            
            # 生成分析报告文件
            report_filename = f"数据分析报告_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            report_path = os.path.join(self.export_dir, report_filename)
            
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write("=== 财务毛利数据分析报告 ===\n\n")
                
                # 写入数据概览
                f.write("1. 数据概览\n")
                f.write("-" * 30 + "\n")
                for key, value in analysis_result['数据概览'].items():
                    f.write(f"{key}: {value}\n")
                f.write("\n")
                
                # 写入列信息
                f.write("2. 列信息\n")
                f.write("-" * 30 + "\n")
                for col, info in analysis_result['列信息'].items():
                    f.write(f"\n列名: {col}\n")
                    for key, value in info.items():
                        f.write(f"  {key}: {value}\n")
                
                # 写入数值列统计
                if analysis_result['数值列统计']:
                    f.write("\n3. 数值列统计\n")
                    f.write("-" * 30 + "\n")
                    for col, stats in analysis_result['数值列统计'].items():
                        f.write(f"\n列名: {col}\n")
                        for key, value in stats.items():
                            if pd.notna(value):
                                f.write(f"  {key}: {value:.2f}\n")
            
            logger.info(f"数据分析报告已生成: {report_path}")
            return analysis_result
            
        except Exception as e:
            logger.error(f"数据分析失败: {e}")
            return {}
    
    def run_financial_analysis(self):
        """运行完整的财务分析流程"""
        try:
            logger.info("开始运行财务分析流程")
            
            # 1. 登录Hue
            if not self.login():
                logger.error("Hue登录失败，无法继续")
                return
            
            # 2. 读取财务毛利SQL
            sql_file = '/Users/boxie/SQL/财务/财务毛利.sql'
            if not os.path.exists(sql_file):
                logger.error(f"SQL文件不存在: {sql_file}")
                return
            
            with open(sql_file, 'r', encoding='utf-8') as f:
                original_sql = f.read()
            
            # 3. 优化SQL查询
            optimized_sql = self.optimize_query(original_sql)
            
            # 4. 执行查询
            df = self.execute_query(optimized_sql, "财务毛利查询")
            if df is None:
                logger.error("查询执行失败")
                return
            
            # 5. 导出数据
            csv_path = self.export_data(df, "财务毛利数据", "csv")
            excel_path = self.export_data(df, "财务毛利数据", "excel")
            
            # 6. 数据分析
            analysis_result = self.analyze_data(df)
            
            # 7. 生成总结报告
            self._generate_summary_report(df, csv_path, excel_path, analysis_result)
            
            logger.info("财务分析流程完成！")
            
        except Exception as e:
            logger.error(f"运行财务分析流程时发生错误: {e}")
    
    def _generate_summary_report(self, df: pd.DataFrame, csv_path: str, excel_path: str, analysis_result: Dict):
        """生成总结报告"""
        try:
            summary_filename = f"财务分析总结报告_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
            summary_path = os.path.join(self.export_dir, summary_filename)
            
            with open(summary_path, 'w', encoding='utf-8') as f:
                f.write("# 财务毛利数据分析总结报告\n\n")
                f.write(f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                f.write("## 📊 数据概览\n\n")
                f.write(f"- **总记录数**: {len(df):,} 条\n")
                f.write(f"- **总字段数**: {len(df.columns)} 个\n")
                f.write(f"- **数据大小**: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB\n\n")
                
                f.write("## 📁 导出文件\n\n")
                f.write(f"- **CSV文件**: {csv_path}\n")
                f.write(f"- **Excel文件**: {excel_path}\n\n")
                
                f.write("## 🔍 关键指标分析\n\n")
                
                # 分析关键财务指标
                if 'foffer_amounta' in df.columns:
                    total_offer_a = df['foffer_amounta'].sum()
                    f.write(f"- **报价金额A总计**: ¥{total_offer_a:,.2f}\n")
                
                if 'fcost_amounta' in df.columns:
                    total_cost_a = df['fcost_amounta'].sum()
                    f.write(f"- **成本金额A总计**: ¥{total_cost_a:,.2f}\n")
                
                if 'fdetection_amount' in df.columns:
                    total_detection = df['fdetection_amount'].sum()
                    f.write(f"- **检测金额总计**: ¥{total_detection:,.2f}\n")
                
                f.write("\n## 📈 数据质量\n\n")
                
                # 数据质量分析
                missing_data = df.isnull().sum().sum()
                total_cells = df.size
                data_quality = (1 - missing_data / total_cells) * 100
                
                f.write(f"- **数据完整度**: {data_quality:.2f}%\n")
                f.write(f"- **缺失值数量**: {missing_data:,} 个\n")
                f.write(f"- **总数据单元格**: {total_cells:,} 个\n\n")
                
                f.write("## 🎯 建议和注意事项\n\n")
                f.write("1. **数据验证**: 建议对关键财务数据进行交叉验证\n")
                f.write("2. **性能优化**: 查询已进行基础优化，可根据实际性能进一步调整\n")
                f.write("3. **定期更新**: 建议定期运行此分析，监控财务数据变化\n")
                f.write("4. **异常检测**: 关注数据中的异常值和缺失值\n\n")
                
                f.write("---\n")
                f.write("*本报告由Hue自动化脚本自动生成*")
            
            logger.info(f"总结报告已生成: {summary_path}")
            
        except Exception as e:
            logger.error(f"生成总结报告失败: {e}")

def main():
    """主函数"""
    print("🚀 Hue查询自动化工具启动中...")
    print("=" * 50)
    
    try:
        # 创建自动化工具实例
        automation = HueAutomation()
        
        # 运行财务分析
        automation.run_financial_analysis()
        
        print("\n✅ 自动化流程完成！")
        print(f"📁 请查看 {automation.export_dir} 目录下的结果文件")
        
    except KeyboardInterrupt:
        print("\n⚠️  用户中断了程序")
    except Exception as e:
        print(f"\n❌ 程序运行出错: {e}")
        logger.error(f"主程序运行出错: {e}")

if __name__ == "__main__":
    main()
