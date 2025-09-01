from sales_revenue_predictor import SalesRevenuePredictor
from enhanced_cost_revenue_analyzer import EnhancedCostRevenueAnalyzer
from enhanced_analyzer_with_dates import EnhancedCostRevenueAnalyzerWithDate
from revenue_analyze_base_data import RevenueAnalyzeBaseData
from weekly_revenue_data import WeeklyRevenueDataProcessor
from weekly_cost_export import WeeklyCostExport
from cost_predictor import CostPredictor
from b2b_sale_predictor import B2BSalesPredictor


## 1. generator revenue prediction data (Pax Data in Public)
# generate revenue by using pax*price
predictor = SalesRevenuePredictor()
predictor.run_full_prediction()


##2. prepare cost data from linear model
# weekly event accounting data
should_read_excel = False
event_revenue = WeeklyRevenueDataProcessor()
event_revenue.process(read_from_excel=should_read_excel)

# weekly cost export data
exporter = WeeklyCostExport()
exporter.run_weekly_cost_export(read_from_excel=should_read_excel)

# join weekly accounting and cost export
analyzer = RevenueAnalyzeBaseData()
analyzer.run_base_data_preparation()

# ##  linear model
# analyzer1 = EnhancedCostRevenueAnalyzerWithDate(
#         input_filename='revenue_analyze_base_data.xlsx',
#         input_sheetname='Aggregated_Data',
#         start_date='2024-06-01',  # 起始日期
#         end_date='2026-06-09',    # 终止日期
#         date_column='StartOfWeek'  # 日期列名
#     )
# analyzer1.load_data()
# analyzer1.analyze_relationship_by_currency(output_excel_filename='linear_model_analysis_results_2023.xlsx')
# analyzer1.generate_summary_report()
# analyzer1.summarize_model_parameters(output_filename='model_parameters_summary_2023.xlsx')


# linear model the cost items with revenue
analyzer = EnhancedCostRevenueAnalyzer(input_filename='revenue_analyze_base_data.xlsx',
                                       input_sheetname='Aggregated_Data')
analyzer.load_data()

if analyzer.df is not None:
    # 执行条件分析
    analyzer.perform_conditional_analysis()

    # 汇总参数
    analyzer.summarize_model_parameters()
else:
    print("Data loading failed, please check file path and content.")

# 3. 根据建模参数和收入预测数据生成成本预测
predictor = CostPredictor()
predictor.run_full_prediction()

# 4. 根据收入预测和direct sale预测生成B2B预测
predictor = B2BSalesPredictor()
predictor.run_analysis()