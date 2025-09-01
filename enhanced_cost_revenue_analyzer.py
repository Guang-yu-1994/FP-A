import os
import pandas as pd
import statsmodels.api as sm
import matplotlib.pyplot as plt
import numpy as np
import openpyxl


class EnhancedCostRevenueAnalyzer:
    def __init__(self, input_filename='revenue_analyze_base_data.xlsx', input_sheetname='Aggregated_Data'):
        """
        初始化 CostRevenueAnalyzer 类。

        Args:
            input_filename (str): 输入的 Excel 文件名。
            input_sheetname (str): 输入的 Excel 工作表名。
        """
        self.input_filename = input_filename
        self.input_sheetname = input_sheetname
        # 定义输出目录，确保输入文件也在该目录下
        self.outputs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Revenue Outputs')
        self.file_path = os.path.join(self.outputs_dir, self.input_filename) # 修正了输入文件路径

        self.df = None

        # 定义要分析的成本列，已将 'AP COGS' 更新为 'COGS exc Guide&Coord'
        self.cost_columns = [
            'CF',
            'AP',
            'Guide&Coord',
            'COGS exc Guide&Coord' # 更新了列名
        ]
        self.revenue_column = 'Gross Revenue Local'
        self.currency_column = 'Currency'

        # 初始化参数存储字典
        self.model_parameters = {}

        # 创建 Outputs 目录如果不存在
        os.makedirs(self.outputs_dir, exist_ok=True)

    def load_data(self):
        """
        载入 Excel 数据并进行基础清洗。
        """
        try:
            print(f"Attempting to read file: {self.file_path}")
            self.df = pd.read_excel(self.file_path, sheet_name=self.input_sheetname)
            print("File read successfully!")

            required_cols = [self.revenue_column, self.currency_column] + self.cost_columns
            missing_cols = [col for col in required_cols if col not in self.df.columns]

            if missing_cols:
                print(f"Error: Missing the following required columns in the input file: {missing_cols}")
                self.df = None
                return

            if 'Week Start Date' in self.df.columns:
                self.df['Week Start Date'] = pd.to_datetime(self.df['Week Start Date'])
                self.df.sort_values(by='Week Start Date', inplace=True)

            print(f"Data loaded, total {len(self.df)} rows.")

        except FileNotFoundError:
            print(f"Error: File not found, please check the path: {self.file_path}")
            self.df = None
        except Exception as e:
            print(f"An error occurred while reading the file: {e}")
            self.df = None

    def format_equation_and_stats(self, model, cost_col, revenue_col, has_intercept):
        """
        为带截距和不带截距模型格式化回归方程和统计信息。
        """
        slope = model.params[revenue_col]
        r_squared = model.rsquared
        # 截距项的 P 值可能不存在，需要安全获取
        p_value_slope = model.pvalues[revenue_col]

        if has_intercept:
            constant = model.params['const']
            equation = f"{cost_col} = {constant:.4f} + {slope:.4f} × {revenue_col}"
            p_value_constant = model.pvalues['const']
            p_str_constant = f"p_const < 0.001" if p_value_constant < 0.001 else f"p_const = {p_value_constant:.3f}"
        else:
            equation = f"{cost_col} = {slope:.4f} × {revenue_col} (No Intercept)"
            p_str_constant = "" # 无截距模型没有常数项的 P 值

        p_str_slope = f"p_slope < 0.001" if p_value_slope < 0.001 else f"p_slope = {p_value_slope:.3f}"
        stats_str = f"R² = {r_squared:.3f}, Slope {p_str_slope}"
        if has_intercept:
            stats_str += f", Intercept {p_str_constant}"

        return equation, stats_str

    def perform_conditional_analysis(self, output_excel_filename='conditional_model_results.xlsx'):
        """
        根据截距值执行条件线性回归分析。
        如果截距为负，则使用无截距模型。否则，使用带截距模型。
        """
        if self.df is None:
            print("Data not loaded successfully, cannot proceed with analysis.")
            return

        print("\n===== Starting Conditional Linear Regression Analysis =====")

        currencies = self.df[self.currency_column].unique()
        for currency in currencies:
            currency_name = 'Unknown' if pd.isna(currency) else str(currency)

            # 创建货币专用文件夹
            currency_folder = os.path.join(self.outputs_dir, currency_name)
            os.makedirs(currency_folder, exist_ok=True)

            currency_data_mask = self.df[self.currency_column].isna() if currency_name == 'Unknown' else (
                        self.df[self.currency_column] == currency)
            currency_data = self.df[currency_data_mask]

            print(f"\n--- Analyzing Currency: {currency_name} ---")
            if len(currency_data) < 10:
                print(f"Warning: Insufficient data for currency {currency_name} (less than 10 rows), skipping.")
                continue

            currency_excel_path = os.path.join(currency_folder, f"{currency_name}_{output_excel_filename}")

            with pd.ExcelWriter(currency_excel_path, engine='openpyxl') as writer:
                for cost_col in self.cost_columns:
                    print(f"  Analyzing {cost_col}...")

                    data_for_model = currency_data[[self.revenue_column, cost_col]].dropna()
                    if len(data_for_model) < 5:
                        print(f"  Skipping {cost_col} due to insufficient data points (less than 5).")
                        continue

                    y = data_for_model[cost_col]
                    # 为带截距模型准备自变量
                    X_with_const = sm.add_constant(data_for_model[[self.revenue_column]])
                    # 为无截距模型准备自变量
                    X_no_const = data_for_model[[self.revenue_column]]

                    try:
                        # 首先，尝试使用带截距的模型
                        model_with_intercept = sm.OLS(y, X_with_const).fit()
                        constant_value = model_with_intercept.params['const']

                        model_to_use = None
                        constant_to_store = None
                        slope_to_store = None
                        p_value_slope_to_store = None
                        r_squared_to_store = None
                        has_intercept = False
                        p_value_constant_to_store = None # 初始化常数项的P值

                        if constant_value < 0:
                            # 如果截距为负，则使用无截距模型
                            print(f"    Intercept for {cost_col} is negative ({constant_value:.4f}), switching to no-intercept model.")
                            model_to_use = sm.OLS(y, X_no_const).fit()
                            constant_to_store = 0 # 根据要求，无截距模型常数项记为0
                            has_intercept = False
                            p_value_constant_to_store = None # 无截距模型没有常数项的P值
                        else:
                            # 如果截距为非负，则保留带截距模型
                            print(f"    Intercept for {cost_col} is non-negative ({constant_value:.4f}), retaining intercept model.")
                            model_to_use = model_with_intercept
                            constant_to_store = constant_value
                            has_intercept = True
                            p_value_constant_to_store = model_to_use.pvalues['const']


                        slope_to_store = model_to_use.params[self.revenue_column]
                        p_value_slope_to_store = model_to_use.pvalues[self.revenue_column]
                        r_squared_to_store = model_to_use.rsquared

                        # 存储参数用于后续汇总
                        if currency_name not in self.model_parameters:
                            self.model_parameters[currency_name] = {}
                        self.model_parameters[currency_name][cost_col] = {
                            'slope': slope_to_store,
                            'constant': constant_to_store,
                            'p_value_slope': p_value_slope_to_store,
                            'r_squared': r_squared_to_store,
                            'has_intercept': has_intercept,
                            'p_value_constant': p_value_constant_to_store
                        }


                        # --- 结果保存和绘图 ---
                        sheet_name = cost_col.replace(" ", "_").replace("&", "And")[:31]
                        summary_df_table0 = pd.DataFrame(model_to_use.summary2().tables[0])
                        summary_df_table0.to_excel(writer, sheet_name=sheet_name, index=False, header=False)
                        pd.DataFrame(model_to_use.summary2().tables[1]).to_excel(writer, sheet_name=sheet_name,
                                                                          startrow=len(summary_df_table0) + 2)

                        plt.figure(figsize=(12, 8))
                        plt.scatter(data_for_model[self.revenue_column], y, alpha=0.6, label='Data points')

                        x_line = np.linspace(data_for_model[self.revenue_column].min(),
                                             data_for_model[self.revenue_column].max(), 100)
                        if has_intercept:
                            y_line = model_to_use.predict(sm.add_constant(pd.DataFrame({self.revenue_column: x_line})))
                            plt.plot(x_line, y_line, color='red', linewidth=2, label='Regression line (with intercept)')
                            plt.title(f'{currency_name}: {cost_col} vs {self.revenue_column} (With Intercept)', fontsize=14)
                        else:
                            y_line = model_to_use.predict(pd.DataFrame({self.revenue_column: x_line}))
                            plt.plot(x_line, y_line, color='red', linewidth=2, label='Regression line (no intercept)')
                            plt.title(f'{currency_name}: {cost_col} vs {self.revenue_column} (No Intercept)', fontsize=14)

                        plt.xlabel(self.revenue_column)
                        plt.ylabel(cost_col)

                        equation, stats_str = self.format_equation_and_stats(model_to_use, cost_col, self.revenue_column, has_intercept)
                        textstr = f'{equation}\n{stats_str}'
                        props = dict(boxstyle='round', facecolor='wheat', alpha=0.8)
                        plt.text(0.05, 0.95, textstr, transform=plt.gca().transAxes, fontsize=10,
                                 verticalalignment='top', bbox=props)

                        plt.grid(True, alpha=0.3)
                        plt.legend()
                        plt.tight_layout()

                        plot_filename = f'{sheet_name}_vs_{self.revenue_column.replace(" ", "_")}_scatter.png'
                        plot_path = os.path.join(currency_folder, plot_filename)
                        plt.savefig(plot_path, dpi=300)
                        plt.close()

                    except Exception as e:
                        print(f"  An error occurred while analyzing {cost_col}: {e}")

            print(f"Model results for currency {currency_name} saved to: {currency_excel_path}")

    def summarize_model_parameters(self, output_filename='model_parameters_summary.xlsx'):
        """
        汇总模型的关键参数。

        Args:
            output_filename (str): 输出的汇总Excel文件名。
        """
        if not self.model_parameters:
            print("\nNo model parameters to summarize.")
            return

        print("\nStarting to summarize model parameters...")
        # 更新了要汇总的成本列，将 'AP COGS' 替换为 'COGS exc Guide&Coord'
        target_costs = ['CF', 'AP', 'Guide&Coord', 'COGS exc Guide&Coord']
        summary_data = []

        for currency, cost_models in self.model_parameters.items():
            row_data = {'Currency': currency}
            for cost in target_costs:
                if cost in cost_models:
                    params = cost_models[cost]
                    row_data[f'{cost}_slope'] = params['slope']
                    row_data[f'{cost}_constant'] = params['constant']
                    row_data[f'{cost}_r_squared'] = params['r_squared']
                    row_data[f'{cost}_p_value_slope'] = params['p_value_slope']
                    row_data[f'{cost}_p_value_constant'] = params['p_value_constant']
                    row_data[f'{cost}_has_intercept'] = 'Yes' if params['has_intercept'] else 'No'
                else:
                    row_data[f'{cost}_slope'] = None
                    row_data[f'{cost}_constant'] = None
                    row_data[f'{cost}_r_squared'] = None
                    row_data[f'{cost}_p_value_slope'] = None
                    row_data[f'{cost}_p_value_constant'] = None
                    row_data[f'{cost}_has_intercept'] = None
            summary_data.append(row_data)

        if not summary_data:
            print("No data to summarize.")
            return

        summary_df = pd.DataFrame(summary_data)
        output_path = os.path.join(self.outputs_dir, output_filename)
        summary_df.to_excel(output_path, index=False)

        print(f"Model parameter summary saved to: {output_path}")
        print("\nParameter summary preview:")
        print(summary_df.to_string(index=False, float_format='%.6f'))


if __name__ == "__main__":
    # 确保 'Revenue Outputs' 目录存在，并在其中放置 'revenue_analyze_base_data.xlsx'
    analyzer = EnhancedCostRevenueAnalyzer(input_filename='revenue_analyze_base_data.xlsx', input_sheetname='Aggregated_Data')
    analyzer.load_data()

    if analyzer.df is not None:
        # 执行条件分析
        analyzer.perform_conditional_analysis()

        # 汇总参数
        analyzer.summarize_model_parameters()
    else:
        print("Data loading failed, please check file path and content.")
