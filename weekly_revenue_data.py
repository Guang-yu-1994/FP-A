import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple


class WeeklyRevenueDataProcessor:
    """
    Weekly Revenue Data Processor 类
    用于处理收入数据的周度聚合
    """

    def __init__(self, base_dir: Optional[str] = None):
        """
        初始化类

        Args:
            base_dir: 基础目录路径，如果为None则使用脚本所在目录
        """
        if base_dir is None:
            self.base_dir = os.path.dirname(os.path.abspath(__file__))
        else:
            self.base_dir = base_dir

        self.input_dir = os.path.join(self.base_dir, 'Revenue Inputs')
        self.output_dir = os.path.join(self.base_dir, 'Revenue Outputs')
        # 将公共数据库路径设置为用户指定的位置
        self.public_dir = r"C:\City Experience\Public Data Base"  # 用户已将文件移动到此目录

        # 确保输出目录和公共数据库目录存在
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.public_dir, exist_ok=True)  # 确保公共目录存在

        # 必要的列名
        self.required_columns = ['Event Date', 'Tour ID', 'Event Name', 'Currency']
        self.groupby_columns = ['StartOfWeek', 'Tour ID', 'Event Name', 'Currency']

        # 数据存储
        self.original_data = None
        self.weekly_summary = None
        self.numeric_columns = []

    def get_start_of_week(self, date) -> Optional[datetime.date]:
        """
        获取指定日期所在周的周一日期

        Args:
            date: 输入日期

        Returns:
            周一的日期，如果输入为空则返回None
        """
        if pd.isna(date):
            return None

        # 确保输入是datetime对象
        if isinstance(date, str):
            date = pd.to_datetime(date)

        # 计算周一的日期 (weekday() 返回0-6，0是周一)
        days_since_monday = date.weekday()
        start_of_week = date - timedelta(days=days_since_monday)

        return start_of_week.date()

    def is_numeric_column(self, series: pd.Series) -> bool:
        """
        判断一个Series是否为数值型数据

        Args:
            series: 要检查的pandas Series

        Returns:
            True如果是数值型，False否则
        """
        # 排除空值
        non_null_series = series.dropna()

        if len(non_null_series) == 0:
            return False

        # 检查是否为数值类型
        return pd.api.types.is_numeric_dtype(non_null_series)

    def load_data(self, filename: str = 'Revenue Basic Data.xlsx',
                  sheet_name: str = 'Revenue Data',
                  read_from_excel: bool = True) -> bool:
        """
        加载数据文件，可选从Excel或CSV读取。
        如果从Excel读取，则同时保存为CSV格式。

        Args:
            filename: 输入文件名 (Excel文件名)
            sheet_name: 工作表名称
            read_from_excel (bool): 如果为True，则从Excel读取并保存为CSV；
                                    如果为False，则直接从CSV读取。

        Returns:
            True如果加载成功，False否则
        """
        # 假设CSV文件名基于Excel文件名，但扩展名为.csv
        csv_filename = 'Revenue Basic Data.csv'

        excel_file_path = os.path.join(self.input_dir, filename)
        csv_file_path = os.path.join(self.input_dir, csv_filename)

        try:
            if read_from_excel:
                print(f"选择从Excel重新读取。正在读取文件: {excel_file_path}")
                if not os.path.exists(excel_file_path):
                    print(f"错误：找不到Excel文件 {excel_file_path}")
                    return False

                self.original_data = pd.read_excel(excel_file_path, sheet_name=sheet_name)
                print(f"成功从Excel读取数据，共 {len(self.original_data)} 行")

                # 读取后在同个文件夹输出CSV格式
                print(f"正在将数据保存为CSV格式: {csv_file_path}")
                self.original_data.to_csv(csv_file_path, index=False, encoding='utf-8')
                print("CSV文件保存成功。")
            else:
                print(f"选择从CSV读取。正在读取文件: {csv_file_path}")
                if not os.path.exists(csv_file_path):
                    print(f"错误：找不到CSV文件 {csv_file_path}。请先运行一次从Excel读取的模式。")
                    return False

                self.original_data = pd.read_csv(csv_file_path, encoding='utf-8')
                print(f"成功从CSV读取数据，共 {len(self.original_data)} 行")

            return True

        except FileNotFoundError:
            print(f"错误：文件操作失败，请检查文件路径和名称。")
            return False
        except Exception as e:
            print(f"读取或保存文件时发生错误: {str(e)}")
            return False

    def validate_data(self) -> bool:
        """
        验证数据是否包含必要的列

        Returns:
            True如果验证通过，False否则
        """
        if self.original_data is None:
            print("错误：尚未加载数据")
            return False

        missing_columns = [col for col in self.required_columns
                           if col not in self.original_data.columns]

        if missing_columns:
            print(f"错误：缺少必要的列: {missing_columns}")
            print(f"可用的列: {list(self.original_data.columns)}")
            return False

        return True

    def prepare_data(self) -> bool:
        """
        准备数据：添加StartOfWeek列，识别数值型列

        Returns:
            True如果准备成功，False否则
        """
        if not self.validate_data():
            return False

        try:
            # 添加StartOfWeek列
            print("正在添加StartOfWeek列...")
            self.original_data['Event Date'] = pd.to_datetime(self.original_data['Event Date'])
            self.original_data['StartOfWeek'] = self.original_data['Event Date'].apply(self.get_start_of_week)

            # 识别数值型列
            print("正在识别数值型列...")
            self.numeric_columns = []

            for col in self.original_data.columns:
                if col not in self.groupby_columns and col != 'Event Date':
                    if self.is_numeric_column(self.original_data[col]):
                        self.numeric_columns.append(col)
                        print(f"  - 数值型列: {col}")
                    else:
                        print(f"  - 非数值型列: {col} (将被忽略)")

            if not self.numeric_columns:
                print("警告：没有找到可聚合的数值型列")
                return False

            return True

        except Exception as e:
            print(f"准备数据时发生错误: {str(e)}")
            return False

    def aggregate_data(self) -> bool:
        """
        执行数据聚合

        Returns:
            True如果聚合成功，False否则
        """
        if not self.numeric_columns:
            print("错误：没有可聚合的数值型列")
            return False

        try:
            print("正在进行分组聚合...")

            # 准备聚合字典
            agg_dict = {col: 'sum' for col in self.numeric_columns}

            # 执行分组聚合
            self.weekly_summary = self.original_data.groupby(self.groupby_columns).agg(agg_dict).reset_index()

            # 添加记录计数
            event_counts = self.original_data.groupby(self.groupby_columns).size().reset_index(name='Event_Count')
            self.weekly_summary = self.weekly_summary.merge(event_counts, on=self.groupby_columns)

            # 排序
            self.weekly_summary = self.weekly_summary.sort_values(['StartOfWeek', 'Tour ID', 'Event Name', 'Currency'])

            print(f"聚合完成，共生成 {len(self.weekly_summary)} 行周度汇总数据")
            return True

        except Exception as e:
            print(f"聚合数据时发生错误: {str(e)}")
            return False

    def save_results(self, output_filename: str = 'Weekly_Revenue_Data.xlsx') -> bool:
        """
        保存结果到Excel文件

        Args:
            output_filename: 输出文件名

        Returns:
            True如果保存成功，False否则
        """
        if self.original_data is None or self.weekly_summary is None:
            print("错误：没有数据可保存")
            return False

        try:
            output_file = os.path.join(self.output_dir, output_filename)

            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # 保存原始数据（带StartOfWeek列）
                self.original_data.to_excel(writer, sheet_name='Original_Data_with_StartOfWeek', index=False)

                # 保存周度汇总数据
                self.weekly_summary.to_excel(writer, sheet_name='Weekly_Summary', index=False)

            print(f"结果已保存到: {output_file}")
            return True

        except Exception as e:
            print(f"保存文件时发生错误: {str(e)}")
            return False

    def print_summary(self) -> None:
        """
        打印处理汇总信息
        """
        if self.original_data is None or self.weekly_summary is None:
            print("没有数据可显示汇总信息")
            return

        print("\n=== 处理汇总 ===")
        print(f"原始数据行数: {len(self.original_data)}")
        print(f"周度汇总行数: {len(self.weekly_summary)}")
        print(f"聚合的数值型列: {', '.join(self.numeric_columns)}")
        print(f"时间范围: {self.original_data['StartOfWeek'].min()} 到 {self.original_data['StartOfWeek'].max()}")

        # 显示前几行作为示例
        print("\n=== 周度汇总数据示例 ===")
        print(self.weekly_summary.head())

    def process(self, input_filename: str = 'Revenue Basic Data.xlsx',
                sheet_name: str = 'Revenue Data',
                output_filename: str = 'Weekly_Revenue_Data.xlsx',
                read_from_excel: bool = True) -> bool:
        """
        执行完整的处理流程，并提供是否重新读入Excel的选项。

        Args:
            input_filename: 输入文件名 (Excel文件名)
            sheet_name: 工作表名称
            output_filename: 输出文件名
            read_from_excel (bool): 如果为True，则从Excel读取并保存为CSV；
                                    如果为False，则直接从CSV读取。

        Returns:
            True如果处理成功，False否则
        """
        print("开始处理Weekly Revenue Data...")

        # 加载数据 (根据read_from_excel参数)
        if not self.load_data(input_filename, sheet_name, read_from_excel=read_from_excel):
            return False

        # 准备数据
        if not self.prepare_data():
            return False

        # 聚合数据
        if not self.aggregate_data():
            return False

        # 保存结果
        if not self.save_results(output_filename):
            return False

        # 显示汇总信息
        self.print_summary()

        print("处理完成！")
        return True

    def get_original_data(self) -> Optional[pd.DataFrame]:
        """
        获取原始数据

        Returns:
            原始数据DataFrame，如果没有数据则返回None
        """
        return self.original_data

    def get_weekly_summary(self) -> Optional[pd.DataFrame]:
        """
        获取周度汇总数据

        Returns:
            周度汇总数据DataFrame，如果没有数据则返回None
        """
        return self.weekly_summary

    def get_numeric_columns(self) -> List[str]:
        """
        获取数值型列列表

        Returns:
            数值型列名列表
        """
        return self.numeric_columns.copy()


def main():
    """
    主函数示例
    """
    # 创建实例
    processor = WeeklyRevenueDataProcessor()

    # 设定可选参数：是否重新读入Excel
    # 用户可以在这里修改 True/False 来控制行为
    # True: 从Excel读取并生成CSV
    # False: 直接从已生成的CSV读取
    should_read_excel = True  # 默认设置为True，首次运行时会从Excel读取并生成CSV

    # 执行完整处理流程
    success = processor.process(read_from_excel=should_read_excel)

    if success:
        print("Weekly Revenue Data 处理成功完成！")
    else:
        print("处理过程中遇到错误，请检查日志信息。")


if __name__ == "__main__":
    main()
