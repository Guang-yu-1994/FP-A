import os
import pandas as pd
import logging
import numpy as np  # Import numpy for numerical type selection


class RevenueAnalyzeBaseData:
    """
    Revenue Analyze Base Data processing class
    Used to prepare base data for revenue analysis, including the merger of Weekly cost export and Weekly revenue data.
    """

    def __init__(self):
        # Set base directory and paths
        self.BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        self.INPUT_DIR = os.path.join(self.BASE_DIR, 'Revenue Inputs')
        self.OUTPUT_DIR = os.path.join(self.BASE_DIR, 'Revenue Outputs')
        self.Public_DIR = r"C:\City Experience\Public Data Base"

        # Set up logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        # Ensure output directory exists
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)

    def load_weekly_cost_export(self):
        """
        Reads Weekly cost export data.

        Returns:
            DataFrame: Weekly cost export data.
        """
        try:
            file_path = os.path.join(self.OUTPUT_DIR, 'weekly_cost_export_result.xlsx')

            self.logger.info(f"正在读取Weekly cost export文件: {file_path}")

            if not os.path.exists(file_path):
                raise FileNotFoundError(f"文件不存在: {file_path}")

            df = pd.read_excel(file_path)

            self.logger.info(f"成功读取Weekly cost export数据，共{len(df)}行，{len(df.columns)}列")
            self.logger.info(f"列名: {list(df.columns)}")

            return df

        except Exception as e:
            self.logger.error(f"读取Weekly cost export文件时出错: {str(e)}")
            raise

    def load_weekly_revenue_data(self):
        """
        Reads Weekly revenue data.

        Returns:
            DataFrame: Weekly revenue data.
        """
        try:
            file_path = os.path.join(self.OUTPUT_DIR, 'Weekly_Revenue_Data.xlsx')

            self.logger.info(f"正在读取Weekly revenue data文件: {file_path}")

            if not os.path.exists(file_path):
                raise FileNotFoundError(f"文件不存在: {file_path}")

            # Read the 'Weekly_Summary' worksheet
            df = pd.read_excel(file_path, sheet_name='Weekly_Summary')

            self.logger.info(f"成功读取Weekly revenue data数据，共{len(df)}行，{len(df.columns)}列")
            self.logger.info(f"列名: {list(df.columns)}")

            return df

        except Exception as e:
            self.logger.error(f"读取Weekly revenue data文件时出错: {str(e)}")
            raise

    def validate_merge_columns(self, df_revenue, df_cost_export):
        """
        Validates the existence of columns required for merging.

        Args:
            df_revenue: Weekly revenue data.
            df_cost_export: Weekly cost export data.
        """
        # Check required columns for Weekly revenue data
        revenue_required = ['StartOfWeek', 'Tour ID', 'Currency']
        revenue_missing = [col for col in revenue_required if col not in df_revenue.columns]

        if revenue_missing:
            self.logger.error(f"Weekly revenue data缺少列: {revenue_missing}")
            self.logger.info(f"Weekly revenue data实际列名: {list(df_revenue.columns)}")
            raise ValueError(f"Weekly revenue data缺少必要列: {revenue_missing}")

        # Check required columns for Weekly cost export
        cost_export_required = ['StartOfWeek', 'Event ID', 'Currency']
        cost_export_missing = [col for col in cost_export_required if col not in df_cost_export.columns]

        if cost_export_missing:
            self.logger.error(f"Weekly cost export缺少列: {cost_export_missing}")
            self.logger.info(f"Weekly cost export实际列名: {list(df_cost_export.columns)}")
            raise ValueError(f"Weekly cost export缺少必要列: {cost_export_missing}")

        self.logger.info("合并列验证通过")

    def prepare_data_for_merge(self, df_revenue, df_cost_export):
        """
        Prepares data for merging.

        Args:
            df_revenue: Weekly revenue data.
            df_cost_export: Weekly cost export data.

        Returns:
            tuple: Processed DataFrames.
        """
        # Ensure StartOfWeek column is in date format
        df_revenue['StartOfWeek'] = pd.to_datetime(df_revenue['StartOfWeek'])
        df_cost_export['StartOfWeek'] = pd.to_datetime(df_cost_export['StartOfWeek'])

        # Ensure ID columns are in string format for matching
        df_revenue['Tour ID'] = df_revenue['Tour ID'].astype(str)
        df_cost_export['Event ID'] = df_cost_export['Event ID'].astype(str)

        # Ensure Currency column is in string format
        df_revenue['Currency'] = df_revenue['Currency'].astype(str)
        df_cost_export['Currency'] = df_cost_export['Currency'].astype(str)

        self.logger.info("数据类型转换完成")

        return df_revenue, df_cost_export

    def merge_data(self, df_revenue, df_cost_export):
        """
        Merges Weekly revenue data and Weekly cost export data.

        Args:
            df_revenue: Weekly revenue data.
            df_cost_export: Weekly cost export data.

        Returns:
            DataFrame: Merged data.
        """
        try:
            # Validate merge columns
            self.validate_merge_columns(df_revenue, df_cost_export)

            # Prepare data
            df_revenue, df_cost_export = self.prepare_data_for_merge(df_revenue, df_cost_export)

            # Perform left join
            self.logger.info("开始执行左连接...")

            merged_df = pd.merge(
                df_revenue,
                df_cost_export,
                left_on=['StartOfWeek', 'Tour ID', 'Currency'],
                right_on=['StartOfWeek', 'Event ID', 'Currency'],
                how='left',
                suffixes=('_revenue', '_cost_export')
            )

            self.logger.info(f"合并完成，结果共{len(merged_df)}行")

            # Display merge statistics
            total_revenue_records = len(df_revenue)
            matched_records = merged_df['Event ID'].notna().sum()
            unmatched_records = total_revenue_records - matched_records

            self.logger.info(f"合并统计:")
            self.logger.info(f"  - 总的revenue记录: {total_revenue_records}")
            self.logger.info(f"  - 成功匹配记录: {matched_records}")
            self.logger.info(f"  - 未匹配记录: {unmatched_records}")
            self.logger.info(f"  - 匹配率: {(matched_records / total_revenue_records) * 100:.1f}%")

            return merged_df

        except Exception as e:
            self.logger.error(f"合并数据时出错: {str(e)}")
            raise

    def aggregate_data_by_currency_and_week(self, df):
        """
        Aggregates merged data by Currency and StartOfWeek.
        All numerical columns except 'group_size' are summed.
        Text data columns are automatically ignored.

        Args:
            df (pd.DataFrame): Merged data.

        Returns:
            pd.DataFrame: Aggregated data.
        """
        try:
            self.logger.info("开始按照 'Currency' 和 'StartOfWeek' 聚合数据...")

            # Ensure 'Currency' and 'StartOfWeek' exist
            if 'Currency' not in df.columns or 'StartOfWeek' not in df.columns:
                raise ValueError("DataFrame中缺少 'Currency' 或 'StartOfWeek' 列，无法进行聚合。")

            # Identify all numerical columns
            numerical_cols = df.select_dtypes(include=np.number).columns.tolist()

            # Create aggregation function dictionary, all numerical columns default to sum aggregation
            agg_funcs = {col: 'sum' for col in numerical_cols}

            # As per requirement, 'group_size' column is not summed
            if 'group_size' in agg_funcs:
                self.logger.info("排除 'group_size' 列的sum聚合。")
                del agg_funcs['group_size']

            # Filter out columns not present in the current DataFrame
            cols_to_aggregate = {col: func for col, func in agg_funcs.items() if col in df.columns}

            # Perform aggregation
            # reset_index() converts 'Currency' and 'StartOfWeek' from index to columns
            aggregated_df = df.groupby(['Currency', 'StartOfWeek']).agg(cols_to_aggregate).reset_index()

            self.logger.info(f"数据聚合完成，结果共{len(aggregated_df)}行。")
            return aggregated_df

        except Exception as e:
            self.logger.error(f"聚合数据时出错: {str(e)}")
            raise

    def save_base_data(self, merged_df, aggregated_df, filename='revenue_analyze_base_data.xlsx'):
        """
        Saves merged base data and aggregated data to different worksheets in the same Excel file.

        Args:
            merged_df: Original merged DataFrame.
            aggregated_df: Aggregated DataFrame.
            filename: File name.
        """
        try:
            output_path = os.path.join(self.OUTPUT_DIR, filename)

            # Use 'openpyxl' engine as requested
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Save original merged data to 'Merged_Data' worksheet
                merged_df.to_excel(writer, sheet_name='Merged_Data', index=False)
                self.logger.info(f"原始合并数据已保存到 '{output_path}' 的 'Merged_Data' 工作表。")

                # Save aggregated data to 'Aggregated_Data' worksheet
                aggregated_df.to_excel(writer, sheet_name='Aggregated_Data', index=False)
                self.logger.info(f"聚合数据已保存到 '{output_path}' 的 'Aggregated_Data' 工作表。")

        except Exception as e:
            self.logger.error(f"保存文件时出错: {str(e)}")
            raise

    def create_summary_report(self, df):
        """
        Creates a data summary report.

        Args:
            df: Merged data.
        """
        try:
            self.logger.info("=== 数据摘要报告 ===")
            self.logger.info(f"总行数: {len(df)}")
            self.logger.info(f"总列数: {len(df.columns)}")

            # Statistics by week
            if 'StartOfWeek' in df.columns:
                week_summary = df.groupby('StartOfWeek').size()
                self.logger.info(f"覆盖周数: {len(week_summary)}")
                self.logger.info(f"日期范围: {df['StartOfWeek'].min()} 到 {df['StartOfWeek'].max()}")

            # Statistics by currency
            if 'Currency' in df.columns:
                currency_summary = df['Currency'].value_counts()
                self.logger.info(f"货币类型: {list(currency_summary.index)}")

            # Check for null values
            null_summary = df.isnull().sum()
            null_columns = null_summary[null_summary > 0]
            if len(null_columns) > 0:
                self.logger.info("包含空值的列:")
                for col, count in null_columns.items():
                    self.logger.info(f"  - {col}: {count} ({count / len(df) * 100:.1f}%)")
            else:
                self.logger.info("没有包含空值的列。")

        except Exception as e:
            self.logger.error(f"生成摘要报告时出错: {str(e)}")

    def run_base_data_preparation(self):
        """
        Executes the complete base data preparation process.

        Returns:
            DataFrame: The final aggregated data.
        """
        try:
            self.logger.info("开始执行Revenue Analyze基础数据准备流程...")

            # 1. Read Weekly cost export data
            cost_export_data = self.load_weekly_cost_export()

            # 2. Read Weekly revenue data
            revenue_data = self.load_weekly_revenue_data()

            # 3. Merge data
            merged_data = self.merge_data(revenue_data, cost_export_data)

            # 4. New aggregation program: aggregate merged data by Currency and WeekOfStart
            #    Note: Here, 'WeekOfStart' is assumed to be 'StartOfWeek' to maintain consistency with existing code.
            aggregated_data = self.aggregate_data_by_currency_and_week(merged_data)

            # 5. Generate summary report (using aggregated data)
            self.create_summary_report(aggregated_data)

            # 6. Save results (save original merged data and aggregated data to different worksheets)
            self.save_base_data(merged_data, aggregated_data)

            self.logger.info("Revenue Analyze基础数据准备流程执行完成!")

            return aggregated_data  # Return aggregated data

        except Exception as e:
            self.logger.error(f"执行基础数据准备流程时出错: {str(e)}")
            raise


# Example usage
if __name__ == "__main__":
    # Create instance
    analyzer = RevenueAnalyzeBaseData()

    # Execute full process
    try:
        result = analyzer.run_base_data_preparation()
        print("\n基础数据准备和聚合完成!")
        print(f"结果形状 (聚合后): {result.shape}")
        print("聚合后数据前5行预览:")
        print(result.head())

    except Exception as e:
        print(f"执行出错: {str(e)}")
        import traceback

        traceback.print_exc()
