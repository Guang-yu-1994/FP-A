import pandas as pd
import os
from datetime import datetime, timedelta
import numpy as np
import logging
from dateutil.relativedelta import relativedelta


class CostPredictor:
    """
    Guide&Coord Prediction Class
    增加了将周级别数据拆分到日级别并汇总到月级别的功能。
    增加了时间级别参数，如果传入'weekly'，则从 INPUT_DIR/Weekly 文件夹读取收入数据；
    如果传入'daily'，则从 INPUT_DIR/Daily 文件夹读取收入数据。
    """

    def __init__(self, time_level='weekly'):
        # 设置基础目录和输入/输出路径
        self.BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        self.INPUT_DIR = os.path.join(self.BASE_DIR, 'Revenue Outputs')
        self.OUTPUT_DIR = os.path.join(self.BASE_DIR, 'Cost Outputs')
        self.PUBLIC_DIR = r"C:\City Experience\Public Data Base"

        # 时间级别参数
        self.time_level = time_level.lower()
        if self.time_level not in ['weekly', 'daily']:
            raise ValueError("time_level must be 'weekly' or 'daily'")

        # 设置日志
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        # 确保输出目录存在
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)

        # 数据存储
        self.model_parameters = None
        self.revenue_data = None  # 修改：统一为 revenue_data
        self.cogs_data = None  # 存储周级别的COGS预测数据
        self.daily_cogs_data = None  # 新增：存储日级别的COGS数据
        self.monthly_cogs_summary = None  # 新增：存储月级别的COGS汇总数据
        self.guide_cogs_pivot = None  # 用于存储透视表

    def prepare_model_parameters(self):
        """
        准备 model_parameters_summary 表，动态选择斜率、常数和R方列。
        """
        self.logger.info("正在准备模型参数数据 (包括 R-squared)...")

        excel_path = os.path.join(self.INPUT_DIR, 'model_parameters_summary.xlsx')
        try:
            df = pd.read_excel(excel_path)

            # 动态选择包含 '_slope', '_constant', '_r_squared' 或 'Currency' 的列
            selected_columns = [col for col in df.columns if
                                '_slope' in col or '_constant' in col or '_r_squared' in col or col == 'Currency']

            if 'Currency' not in selected_columns:
                raise ValueError("model_parameters_summary.xlsx 中缺少 'Currency' 列，无法进行模型参数准备。")

            df = df[selected_columns]

            self.logger.info(f"成功读取模型参数数据，共{len(df)}行数据")
            self.logger.info(f"包含货币: {df['Currency'].unique().tolist()}")
            self.logger.info(f"包含模型参数列: {list(df.columns)}")

            self.model_parameters = df
            return df

        except Exception as e:
            self.logger.error(f"读取模型参数数据失败: {e}")
            raise Exception(f"读取模型参数数据失败: {e}")

    def prepare_revenue_data(self):
        """准备收入数据，根据 time_level 从不同子文件夹读取。"""
        self.logger.info(f"正在准备{self.time_level}收入数据...")

        sub_dir = 'Weekly' if self.time_level == 'weekly' else 'Daily'
        excel_path = os.path.join(self.INPUT_DIR, sub_dir, 'sales_revenue_prediction.xlsx')
        try:
            df = pd.read_excel(excel_path, sheet_name='Revenue_Prediction')

            # 确保 Event Date 是日期格式
            df['Event Date'] = pd.to_datetime(df['Event Date'])

            # 按 Currency 和 Event Date 聚合 Revenue
            revenue_data = df.groupby(['Currency', 'Event Date'], as_index=False)['Revenue'].sum()

            self.logger.info(f"成功准备{self.time_level}收入数据，共{len(revenue_data)}行数据")
            self.logger.info(
                f"日期范围: {revenue_data['Event Date'].min().date()} 到 {revenue_data['Event Date'].max().date()}")
            self.logger.info(f"总收入: {revenue_data['Revenue'].sum():,.2f}")

            self.revenue_data = revenue_data
            return revenue_data

        except Exception as e:
            self.logger.error(f"读取销售收入预测数据失败: {e}")
            raise Exception(f"读取销售收入预测数据失败: {e}")

    def get_first_week_of_next_month(self, event_date):
        """获取下个月的第一个周一。"""
        # 获取下个月的第一天
        next_month = event_date + relativedelta(months=1)
        first_day_next_month = next_month.replace(day=1)

        # 找到第一个周一
        days_to_monday = (7 - first_day_next_month.weekday()) % 7
        if first_day_next_month.weekday() == 0:  # 如果第一天就是周一
            first_monday = first_day_next_month
        else:
            first_monday = first_day_next_month + timedelta(days=days_to_monday)

        return first_monday.date()

    def calculate_guide_payment_date(self, row):
        """计算 Guide Payment Date。"""
        currency = row['Currency']
        event_date = row['Event Date']

        if isinstance(event_date, str):
            event_date = pd.to_datetime(event_date)

        if currency in ['EUR', 'GBP']:
            # 欧元和英镑：事件日期后的下个月的第1天
            next_month = event_date + relativedelta(months=1)
            return next_month.replace(day=1).date()

        elif currency in ['CAD', 'USD']:
            # 加元和美元：基于事件日期
            if event_date.day < 15:
                # 如果事件日期在当前月的15号之前，付款日期是当前月的15号
                return event_date.replace(day=15).date()
            else:
                # 如果事件日期在当前月的15号之后，付款日期是下个月的第1天
                next_month = event_date + relativedelta(months=1)
                return next_month.replace(day=1).date()

        else:
            # 如果货币不在预定义列表中，返回 None 或默认值
            self.logger.warning(f"未知货币类型 {currency}，无法计算付款日期")
            return None

    def calculate_ap_payment_date(self, row):
        """计算 AP Payment Date = 事件日期下个月的第7天。"""
        event_date = row['Event Date']

        if isinstance(event_date, str):
            event_date = pd.to_datetime(event_date)

        # 获取下个月的第一天
        next_month = event_date + relativedelta(months=1)

        # 设置为下个月的7号
        ap_payment_date = next_month.replace(day=7).date()

        return ap_payment_date

    def prepare_cogs_data(self):
        """
        准备 COGS Generated from Revenue 表，动态添加预测列。
        """
        self.logger.info("正在准备COGS数据...")

        if self.revenue_data is None or self.model_parameters is None:
            raise Exception("请先准备revenue_data和model_parameters数据")

        # 1. 左连接 revenue_data 和 model_parameters，连接键为 Currency
        merged_df = pd.merge(
            self.revenue_data,
            self.model_parameters,
            on='Currency',
            how='left'
        )

        self.logger.info(f"合并后共{len(merged_df)}行数据")

        # 动态添加预测列
        prediction_columns = []
        for col in self.model_parameters.columns:
            if col.endswith('_slope'):
                base_name = col.replace('_slope', '')
                constant_col = f"{base_name}_constant"
                if constant_col in self.model_parameters.columns:
                    prediction_col_name = base_name  # 例如，从 'Guide&Coord_slope' 得到 'Guide&Coord'

                    # 计算新的预测列
                    merged_df[prediction_col_name] = np.where(
                        merged_df['Revenue'] == 0,
                        0,
                        np.maximum(0, merged_df['Revenue'] * merged_df[col] + merged_df[constant_col])
                    )
                    prediction_columns.append(prediction_col_name)
                    self.logger.info(f"已动态新增预测列: {prediction_col_name}")

        # --- AP adj 的新逻辑 ---
        self.logger.info("开始计算 AP adj (调整后 AP)...")

        # 检查调整逻辑所需的列是否存在
        # 已将 'AP COGS' 更新为 'COGS exc Guide&Coord'
        required_cols_for_adj = ['AP', 'COGS exc Guide&Coord', 'CF', 'AP_r_squared'] # 更新了列名
        if all(col in merged_df.columns for col in required_cols_for_adj):
            # 如果 AP 模型的 R-squared > 0.8，则使用直接 AP 预测。
            # 否则，使用 COGS exc Guide&Coord - CF。
            merged_df['AP adj'] = np.where(
                merged_df['AP_r_squared'] > 0.8,
                merged_df['AP'],
                merged_df['COGS exc Guide&Coord'] - merged_df['CF'] # 更新了列名
            )

            # 确保结果不为负
            merged_df['AP adj'] = np.maximum(0, merged_df['AP adj'])

            prediction_columns.append('AP adj')  # 添加到预测列表
            self.logger.info("AP adj 列已成功计算。")

        else:
            missing_cols = [col for col in required_cols_for_adj if col not in merged_df.columns]
            self.logger.warning(f"无法计算 AP adj，因为缺少以下列: {missing_cols}")

        # 计算付款日期
        self.logger.info("正在计算付款日期...")
        merged_df['Guide Payment Date'] = merged_df.apply(self.calculate_guide_payment_date, axis=1)
        merged_df['AP Payment Date'] = merged_df.apply(self.calculate_ap_payment_date, axis=1)
        merged_df['CF Payment Date'] = merged_df.apply(self.calculate_cf_payment_date, axis=1)

        # 排序数据
        merged_df = merged_df.sort_values(['Currency', 'Event Date'])

        # 选择最终列
        final_columns = ['Currency', 'Event Date', 'Revenue'] + prediction_columns + \
                        ['Guide Payment Date', 'AP Payment Date', 'CF Payment Date']
        self.cogs_data = merged_df[final_columns]

        self.logger.info(f"COGS数据准备完成，共{len(self.cogs_data)}行数据")
        self.logger.info(f"包含预测列: {prediction_columns}")

        return self.cogs_data

    def resample_to_daily_and_monthly(self):
        """
        将周级别数据拆分到日级别并汇总到月级别。
        如果 time_level 为 'daily'，则直接使用 cogs_data 作为 daily_cogs_data。
        """
        if self.cogs_data is None:
            raise Exception("请先准备COGS数据")

        if self.time_level == 'daily':
            self.logger.info("时间级别为 'daily'，直接使用现有数据作为日级别数据。")
            self.daily_cogs_data = self.cogs_data.copy()
        else:  # weekly
            self.logger.info("时间级别为 'weekly'，将周级别数据拆分到日级别...")
            # 假设每周数据均匀分布到7天
            daily_rows = []
            for _, row in self.cogs_data.iterrows():
                start_date = row['Event Date']
                for i in range(7):
                    daily_date = start_date + timedelta(days=i)
                    daily_row = row.copy()
                    daily_row['Event Date'] = daily_date
                    # 数值列除以7
                    for col in self.cogs_data.columns:
                        if pd.api.types.is_numeric_dtype(self.cogs_data[col]) and col not in ['Event Date', 'Guide Payment Date', 'AP Payment Date', 'CF Payment Date']:
                            daily_row[col] /= 7
                    daily_rows.append(daily_row)
            self.daily_cogs_data = pd.DataFrame(daily_rows)
            self.daily_cogs_data = self.daily_cogs_data.sort_values(['Currency', 'Event Date'])
            self.logger.info(f"日级别数据拆分完成，共{len(self.daily_cogs_data)}行数据")

        # 汇总到月级别（统一处理）
        self.logger.info("正在汇总到月级别...")
        self.daily_cogs_data['StartOfMonth'] = self.daily_cogs_data['Event Date'].dt.to_period('M').dt.to_timestamp()
        monthly_agg = self.daily_cogs_data.groupby(['Currency', 'StartOfMonth']).sum(numeric_only=True).reset_index()
        self.monthly_cogs_summary = monthly_agg
        self.logger.info(f"月级别汇总完成，共{len(self.monthly_cogs_summary)}行数据")

    def create_guide_cogs_pivot(self):
        """创建 Guide&Coord COGS 透视表，按 Guide Payment Date 列透视。"""
        self.logger.info("正在创建 Guide&Coord COGS 透视表...")
        if self.cogs_data is None:
            raise Exception("没有COGS数据，无法创建透视表。请先运行 prepare_cogs_data。")

        if 'Guide Payment Date' not in self.cogs_data.columns:
            self.logger.warning("警告: 'Guide Payment Date' 列不存在，无法创建透视表。")
            self.guide_cogs_pivot = pd.DataFrame()
            return self.guide_cogs_pivot
        if 'Guide&Coord' not in self.cogs_data.columns:
            self.logger.warning("警告: 'Guide&Coord' 列不存在，无法创建透视表。")
            self.guide_cogs_pivot = pd.DataFrame()
            return self.guide_cogs_pivot

        pivot_data = self.cogs_data.copy()
        pivot_data['Guide Payment Date'] = pd.to_datetime(pivot_data['Guide Payment Date'])

        pivot_table = pd.pivot_table(
            pivot_data,
            values='Guide&Coord',
            index='Currency',
            columns='Guide Payment Date',
            aggfunc='sum',
            fill_value=0
        )

        pivot_table = pivot_table.reset_index()
        # 将列名转换为字符串格式，例如 '2025-06-30'
        pivot_table.columns = ['Currency'] + [col.strftime('%Y-%m-%d') for col in pivot_table.columns[1:]]

        self.logger.info("Guide&Coord COGS 透视表创建完成。")
        self.guide_cogs_pivot = pivot_table
        return pivot_table

    def save_results(self, filename='cost_prediction_output.xlsx'):
        """将结果保存到 Excel 文件中。"""
        if self.cogs_data is None:
            raise Exception("没有可保存的数据，请先运行完整的预测流程")

        output_path = os.path.join(self.OUTPUT_DIR, filename)

        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # 保存原始预测数据（如果是 weekly，则是周级别；如果是 daily，则是日级别，但统一保存到 Cost_Predictions）
            self.cogs_data.to_excel(writer, sheet_name='Cost_Predictions', index=False)
            self.logger.info(f"数据已保存到 '{output_path}' 的 'Cost_Predictions' 工作表。")

            # 保存模型参数
            if self.model_parameters is not None:
                self.model_parameters.to_excel(writer, sheet_name='Model_Parameters_Used', index=False)
                self.logger.info(f"数据已保存到 '{output_path}' 的 'Model_Parameters_Used' 工作表。")

            # 保存收入输入数据
            if self.revenue_data is not None:
                self.revenue_data.to_excel(writer, sheet_name='Revenue_Input', index=False)
                self.logger.info(f"数据已保存到 '{output_path}' 的 'Revenue_Input' 工作表。")

            # 保存 Guide COGS 透视表
            if self.guide_cogs_pivot is not None and not self.guide_cogs_pivot.empty:
                self.guide_cogs_pivot.to_excel(writer, sheet_name='Guide_COGS_Pivot', index=False)
                self.logger.info(f"数据已保存到 '{output_path}' 的 'Guide_COGS_Pivot' 工作表。")
            elif self.guide_cogs_pivot is not None and self.guide_cogs_pivot.empty:
                self.logger.info(f"Guide_COGS_Pivot 为空，未保存到 '{output_path}'。")

            # 新增：保存日级别预测数据
            if self.daily_cogs_data is not None:
                self.daily_cogs_data.to_excel(writer, sheet_name='Daily_Predictions', index=False)
                self.logger.info(f"日级别预测数据已保存到 '{output_path}' 的 'Daily_Predictions' 工作表。")

            # 新增：保存月级别汇总数据
            if self.monthly_cogs_summary is not None:
                # 确保 StartOfMonth 列是日期格式，以便 Excel 识别
                temp_monthly_summary = self.monthly_cogs_summary.copy()
                temp_monthly_summary['StartOfMonth'] = temp_monthly_summary['StartOfMonth'].dt.date
                temp_monthly_summary.to_excel(writer, sheet_name='Monthly_Summary', index=False)
                self.logger.info(f"月级别汇总数据已保存到 '{output_path}' 的 'Monthly_Summary' 工作表。")

        self.logger.info(f"所有结果已保存到: {output_path}")
        return output_path

    def get_summary_by_currency(self):
        """按货币汇总结果，包括动态添加的预测列。"""
        if self.cogs_data is None:
            raise Exception("没有数据可以汇总，请先运行预测流程")

        # 动态识别预测列
        prediction_cols_to_sum = []
        for col in self.cogs_data.columns:
            if col not in ['Currency', 'Event Date', 'Guide Payment Date', 'AP Payment Date'] and \
                    '_slope' not in col and '_constant' not in col and '_r_squared' not in col and \
                    pd.api.types.is_numeric_dtype(self.cogs_data[col]):
                prediction_cols_to_sum.append(col)

        agg_dict = {'Revenue': 'sum'}
        for col in prediction_cols_to_sum:
            agg_dict[col] = 'sum'
        agg_dict['Event Date'] = ['min', 'max']

        summary = self.cogs_data.groupby('Currency').agg(agg_dict).round(2)

        # 扁平化多级列名
        summary.columns = ['_'.join(col).strip() if isinstance(col, tuple) else col for col in summary.columns.values]

        column_renames = {
            'Revenue_sum': 'Total_Revenue',
            'Event Date_min': 'Start_Date',
            'Event Date_max': 'End_Date'
        }
        for col in prediction_cols_to_sum:
            column_renames[f"{col}_sum"] = f"Total_{col.replace(' ', '_')}"

        summary = summary.rename(columns=column_renames).reset_index()

        self.logger.info("\n按货币汇总:")
        self.logger.info(summary.to_string(index=False))

        return summary

    def get_summary_by_payment_date(self):
        """按付款日期汇总 Guide&Coord。"""
        if self.cogs_data is None:
            raise Exception("没有数据可以汇总，请先运行预测流程")

        valid_data = self.cogs_data.dropna(subset=['Guide Payment Date'])

        if 'Guide&Coord' not in valid_data.columns:
            self.logger.warning("警告: 'Guide&Coord' 列不存在于数据中，无法生成按付款日期的汇总。")
            return pd.DataFrame()

        payment_summary = valid_data.groupby(['Currency', 'Guide Payment Date']).agg({
            'Guide&Coord': 'sum',
            'Revenue': 'sum'
        }).round(2).reset_index()

        payment_summary = payment_summary.sort_values(['Currency', 'Guide Payment Date'])

        self.logger.info("\n按付款日期汇总Guide&Coord:")
        self.logger.info(payment_summary.to_string(index=False))

        return payment_summary

    def run_full_prediction(self):
        """运行完整的预测流程。"""
        self.logger.info("开始执行成本预测流程...")

        try:
            # 1. 准备所有数据
            self.prepare_model_parameters()
            self.prepare_revenue_data()
            self.prepare_cogs_data()  # 这一步会生成 self.cogs_data (根据 time_level 是周或日级别数据)

            # 新增：调用拆分和汇总方法，生成日级别和月级别数据
            self.resample_to_daily_and_monthly()

            # 2. 创建透视表 (这里的透视表依然基于 Guide Payment Date)
            self.create_guide_cogs_pivot()

            # 3. 保存结果
            self.save_results()

            # 4. 显示汇总信息
            self.get_summary_by_currency()
            self.get_summary_by_payment_date()

            self.logger.info("\n\n成本预测流程完成！")
            self.logger.info(f"- 总记录数: {len(self.cogs_data)}")
            self.logger.info(f"- 货币种类: {self.cogs_data['Currency'].nunique()}")
            self.logger.info(
                f"- 日期范围: {self.cogs_data['Event Date'].min().date()} 到 {self.cogs_data['Event Date'].max().date()}")

            return self.cogs_data

        except Exception as e:
            self.logger.error(f"预测过程中出现错误: {e}")
            raise


# 示例用法
if __name__ == "__main__":
    # 创建预测器实例，可以指定 time_level
    predictor = CostPredictor(time_level='weekly')  # 或 'daily'

    # 运行完整的预测
    try:
        result = predictor.run_full_prediction()
        print("\n前5行结果:")
        print(result.head().to_string())

        if predictor.daily_cogs_data is not None:
            print("\n前5行日级别结果:")
            print(predictor.daily_cogs_data.head().to_string())

        if predictor.monthly_cogs_summary is not None:
            print("\n前5行月级别汇总结果:")
            print(predictor.monthly_cogs_summary.head().to_string())

    except Exception as e:
        print(f"执行出错: {str(e)}")
        import traceback
        traceback.print_exc()
