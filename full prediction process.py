from sales_revenue_predictor import SalesRevenuePredictor
from unified_revenue_cost_analyzer import UnifiedRevenueCostAnalyzer  # Modified import
from cost_predictor import CostPredictor
from b2b_sale_predictor import B2BSalesPredictor


## 1. generator revenue prediction data (Pax Data in Public)
# generate revenue by using pax*price
predictor = SalesRevenuePredictor()
predictor.run_full_prediction()


##2. prepare cost data from linear model
# weekly event accounting data and weekly cost export data, join them, and linear model
should_read_excel = False
analyzer = UnifiedRevenueCostAnalyzer()  # Unified class
analyzer.run_all(read_from_excel=should_read_excel)  # Call unified run_all


# 3. 根据建模参数和收入预测数据生成成本预测
predictor = CostPredictor()
predictor.run_full_prediction()

# 4. 根据收入预测和direct sale预测生成B2B预测
predictor = B2BSalesPredictor()
predictor.run_analysis()
