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
    """

    def __init__(self):
        # 设置基础目录和输入/输出路径
        self.BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        self.INPUT_DIR = os.path.join(self.BASE_DIR, 'Revenue Outputs')
        self.OUTPUT_DIR = os.path.join(self.BASE_DIR, 'Cost Outputs')
        self.PUBLIC_DIR = r"C:\City Experience\Public Data Base"

        # 设置日志
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        # 确保输出目录存在
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)

        # 数据存储
        self.model_parameters = None
        self.weekly_revenue = None
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

    def prepare_weekly_revenue(self):
        """准备周收入数据。"""
        self.logger.info("正在准备周收入数据...")

        excel_path = os.path.join(self.INPUT_DIR, 'sales_revenue_prediction.xlsx')
        try:
            df = pd.read_excel(excel_path, sheet_name='Revenue_Prediction')

            # 确保 Event Date 是日期格式
            df['Event Date'] = pd.to_datetime(df['Event Date'])

            # 按 Currency 和 Event Date 聚合 Revenue
            weekly_revenue = df.groupby(['Currency', 'Event Date'], as_index=False)['Revenue'].sum()

            self.logger.info(f"成功准备周收入数据，共{len(weekly_revenue)}行数据")
            self.logger.info(
                f"日期范围: {weekly_revenue['Event Date'].min().date()} 到 {weekly_revenue['Event Date'].max().date()}")
            self.logger.info(f"总收入: {weekly_revenue['Revenue'].sum():,.2f}")

            self.weekly_revenue = weekly_revenue
            return weekly_revenue

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

        if self.weekly_revenue is None or self.model_parameters is None:
            raise Exception("请先准备weekly_revenue和model_parameters数据")

        # 1. 左连接 weekly_revenue 和 model_parameters，连接键为 Currency
        merged_df = pd.merge(
            self.weekly_revenue,
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
        # --- AP adj 新逻辑结束 ---

        # 4. 计算 Guide Payment Date
        merged_df['Guide Payment Date'] = merged_df.apply(
            self.calculate_guide_payment_date, axis=1
        )

        # 5. 计算 AP Payment Date
        merged_df['AP Payment Date'] = merged_df.apply(
            self.calculate_ap_payment_date, axis=1
        )

        # 检查是否有未计算付款日期的记录
        null_guide_payment_dates = merged_df['Guide Payment Date'].isnull().sum()
        if null_guide_payment_dates > 0:
            self.logger.warning(f"有{null_guide_payment_dates}条记录无法计算Guide付款日期")

        null_ap_payment_dates = merged_df['AP Payment Date'].isnull().sum()
        if null_ap_payment_dates > 0:
            self.logger.warning(f"有{null_ap_payment_dates}条记录无法计算AP付款日期")

        self.logger.info("COGS数据准备完成")

        # 打印动态添加列的总预测
        for pred_col in prediction_columns:
            if pred_col in merged_df:
                self.logger.info(f"{pred_col} 总预测: {merged_df[pred_col].sum():,.2f}")

        self.cogs_data = merged_df
        return merged_df

    def resample_to_daily_and_monthly(self):
        """
        将周级别数据拆分到日级别并平均，然后汇总到月级别。
        """
        self.logger.info("正在将周级别数据拆分到日级别并汇总到月级别...")

        if self.cogs_data is None:
            raise Exception("没有COGS数据，无法进行日级别拆分。请先运行 prepare_cogs_data。")

        # 1. 筛选所需列，这些列将进行平均分配
        # Currency 列不变，Event Date 变为每天的日期，其余列进行平均
        # 已将 'AP COGS' 更新为 'COGS exc Guide&Coord'
        selected_columns = ['Currency', 'Event Date', 'Revenue', 'Guide&Coord', 'COGS exc Guide&Coord', 'AP', 'CF', 'AP adj'] # 更新了列名
        df_weekly = self.cogs_data[selected_columns].copy()

        daily_records = []
        # 2. 遍历周数据，拆分到日，并平均
        for index, row in df_weekly.iterrows():
            week_start_date = row['Event Date']
            # 确保 Event Date 是 datetime 类型
            if not isinstance(week_start_date, pd.Timestamp):
                week_start_date = pd.to_datetime(week_start_date)

            # 计算这一周的7天日期 (从周一开始)
            week_dates = [week_start_date + timedelta(days=i) for i in range(7)]

            # 计算需要平均的数值列的每日平均值
            # 假设每周有7天，将周总额平均到每天
            num_days_in_week = 7
            avg_revenue = row['Revenue'] / num_days_in_week
            avg_guide_coord = row['Guide&Coord'] / num_days_in_week
            avg_cogs_exc_guide_coord = row['COGS exc Guide&Coord'] / num_days_in_week # 更新了变量名
            avg_ap = row['AP'] / num_days_in_week
            avg_cf = row['CF'] / num_days_in_week
            avg_ap_adj = row['AP adj'] / num_days_in_week

            for day_date in week_dates:
                daily_records.append({
                    'Currency': row['Currency'],
                    'Event Date': day_date,  # 这里是每一天的日期
                    'Revenue': avg_revenue,
                    'Guide&Coord': avg_guide_coord,
                    'COGS exc Guide&Coord': avg_cogs_exc_guide_coord, # 更新了列名
                    'AP': avg_ap,
                    'CF': avg_cf,
                    'AP adj': avg_ap_adj
                })

        # 创建日级别 DataFrame
        df_daily = pd.DataFrame(daily_records)
        # 确保 Event Date 是日期时间类型，以便后续操作
        df_daily['Event Date'] = pd.to_datetime(df_daily['Event Date'])

        # 3. 新建 StartOfMonth 列
        # 使用 .dt.to_period('M') 获取月份周期，再用 .dt.start_time 获取该月的第一个日期
        df_daily['StartOfMonth'] = df_daily['Event Date'].dt.to_period('M').dt.start_time

        self.logger.info(f"日级别数据拆分完成，共 {len(df_daily)} 行。")

        # 4. 聚合为月级别数据
        # 定义需要求和的列
        # 已将 'AP COGS' 更新为 'COGS exc Guide&Coord'
        sum_cols = ['Revenue', 'Guide&Coord', 'COGS exc Guide&Coord', 'AP', 'CF', 'AP adj'] # 更新了列名
        # 按 Currency 和 StartOfMonth 进行分组，并对指定列求和
        monthly_summary = df_daily.groupby(['Currency', 'StartOfMonth'])[sum_cols].sum().reset_index()

        self.logger.info(f"月级别数据汇总完成，共 {len(monthly_summary)} 行。")

        # 将结果存储到类属性中
        self.daily_cogs_data = df_daily
        self.monthly_cogs_summary = monthly_summary

        return df_daily, monthly_summary

    def create_guide_cogs_pivot(self):
        """
        创建 COGS_Generated_from_Revenue 的透视表，以 Currency 为索引，
        Guide Payment Date 为列，Guide&Coord 为值。
        """
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
            # 保存原始周级别预测数据
            self.cogs_data.to_excel(writer, sheet_name='Cost_Predictions', index=False)
            self.logger.info(f"数据已保存到 '{output_path}' 的 'Cost_Predictions' 工作表。")

            # 保存模型参数
            if self.model_parameters is not None:
                self.model_parameters.to_excel(writer, sheet_name='Model_Parameters_Used', index=False)
                self.logger.info(f"数据已保存到 '{output_path}' 的 'Model_Parameters_Used' 工作表。")

            # 保存周收入输入数据
            if self.weekly_revenue is not None:
                self.weekly_revenue.to_excel(writer, sheet_name='Weekly_Revenue_Input', index=False)
                self.logger.info(f"数据已保存到 '{output_path}' 的 'Weekly_Revenue_Input' 工作表。")

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
            self.prepare_weekly_revenue()
            self.prepare_cogs_data()  # 这一步会生成 self.cogs_data (周级别数据)

            # 新增：调用拆分和汇总方法，生成日级别和月级别数据
            self.resample_to_daily_and_monthly()

            # 2. 创建透视表 (这里的透视表依然基于 Guide Payment Date)
            self.create_guide_cogs_pivot()

            # 3. 保存结果
            self.save_results()

            # 4. 显示汇总信息
            self.get_summary_by_currency()
            self.get_summary_by_payment_date()

            self.logger.info("\n成本预测流程完成！")
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
    # 创建预测器实例
    predictor = CostPredictor()

    # 运行完整的预测
    try:
        result = predictor.run_full_prediction()
        print("\n前5行周级别结果:")
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
