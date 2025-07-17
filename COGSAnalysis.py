import os
import pandas as pd
import numpy as np
from datetime import datetime
import xlsxwriter

class COGSAnalysis:
    def __init__(self):
        self.BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        self.INPUT_DIR = os.path.join(self.BASE_DIR, 'COGS Inputs')
        self.OUTPUT_DIR = os.path.join(self.BASE_DIR, 'COGS Outputs')
        self.PUBLIC_DIR = r"C:\City Experience\Public Data Base"
        self.cost_columns = ['Coordinator', 'Food', 'Guide', 'Headsets', 'Misc', 'Tickets', 'Transport']
        
        # Ensure output directory exists
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)
    
    def get_season(self, date):
        """Determine season based on quarter"""
        month = pd.to_datetime(date).month
        return 'High Season' if month in [4,5,6,7,8,9] else 'Low Season'
    
    def add_per_pax_columns(self, df):
        """Add per pax columns for cost metrics"""
        for col in self.cost_columns + ['COGS']:
            df[f'{col} per Pax'] = df[col] / df['Actual Pax'].replace(0, np.nan)
        return df
    
    def prepare_vendor_data(self):
        """Prepare vendor data from AP Prediction.xlsx"""
        df = pd.read_excel(os.path.join(self.INPUT_DIR, 'AP Prediction.xlsx'), sheet_name='Cost Export')
        df = df[['Stage ID', 'Vendor', 'Event Date', 'Event', 'Event ID'] + self.cost_columns]
        
        # Calculate COGS
        df['COGS'] = df[self.cost_columns].sum(axis=1)
        
        # Add Season and Year
        df['Season'] = df['Event Date'].apply(self.get_season)
        df['Year'] = pd.to_datetime(df['Event Date']).dt.year
        
        # Filter for 2024 and later
        df = df[df['Year'] >= 2024]
        
        # Fill missing Vendor values
        df['Vendor'] = df['Vendor'].fillna('Blank Vendor')
        
        return df
    
    def prepare_event_accounting_data(self):
        """Prepare event accounting data from Revenue Basic Data.xlsx"""
        df = pd.read_excel(os.path.join(self.INPUT_DIR, 'Revenue Basic Data.xlsx'), sheet_name='Event Accounting')
        return df[['Stage ID', 'Actual Pax']]
    
    def prepare_merged_data(self):
        """Prepare merged vendor and stage ID level data"""
        vendor_data = self.prepare_vendor_data()
        event_accounting = self.prepare_event_accounting_data()
        
        # Vendor level merged data
        vendor_merged = vendor_data.merge(event_accounting, on='Stage ID', how='left')
        vendor_merged.to_excel(os.path.join(self.OUTPUT_DIR, 'vendor_level_data.xlsx'), index=False)
        
        # Stage ID level data
        group_cols = ['Stage ID', 'Event Date', 'Event', 'Event ID', 'Season', 'Year']
        stage_id_data = vendor_data.groupby(group_cols)[self.cost_columns + ['COGS']].sum().reset_index()
        stage_id_data = stage_id_data.merge(event_accounting, on='Stage ID', how='left')
        stage_id_data.to_excel(os.path.join(self.OUTPUT_DIR, 'stage_id_level_data.xlsx'), index=False)
        
        return vendor_merged, stage_id_data
    
    def format_worksheet(self, worksheet, df, highlight_column=None, highlight_type='std'):
        """Format Excel worksheet with conditional formatting"""
        for col_num, col_name in enumerate(df.columns):
            worksheet.write(0, col_num, col_name)
        
        if highlight_column:
            if highlight_type == 'std':
                # Highlight high standard deviation
                format_red = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
                worksheet.conditional_format(1, df.columns.get_loc(highlight_column), len(df), df.columns.get_loc(highlight_column),
                                          {'type': 'top', 'value': '10', 'format': format_red})
            elif highlight_type == 'change':
                # Highlight high percentage change
                format_red = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
                worksheet.conditional_format(1, df.columns.get_loc(highlight_column), len(df), df.columns.get_loc(highlight_column),
                                          {'type': 'top', 'value': '10', 'format': format_red})
            elif highlight_type == 'minmax':
                # Highlight min and max values
                format_red = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
                format_green = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
                worksheet.conditional_format(1, df.columns.get_loc(highlight_column), len(df), df.columns.get_loc(highlight_column),
                                          {'type': 'top', 'value': '1', 'format': format_red})
                worksheet.conditional_format(1, df.columns.get_loc(highlight_column), len(df), df.columns.get_loc(highlight_column),
                                          {'type': 'bottom', 'value': '1', 'format': format_green})
    
    def per_pax_cogs_analysis(self, stage_id_data):
        """Perform Per Pax COGS analysis"""
        # Aggregate data
        group_cols = ['Event', 'Event ID', 'Season', 'Year']
        agg_data = stage_id_data.groupby(group_cols)[self.cost_columns + ['COGS', 'Actual Pax']].sum().reset_index()
        
        # Add per pax columns
        agg_data = self.add_per_pax_columns(agg_data)
        
        with pd.ExcelWriter(os.path.join(self.OUTPUT_DIR, 'per_pax_cogs_analysis.xlsx'), engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # Longitudinal analysis
            for col in self.cost_columns + ['COGS']:
                pivot = agg_data.pivot_table(
                    values=f'{col} per Pax',
                    index=['Event', 'Event ID', 'Season'],
                    columns='Year'
                ).reset_index()
                
                pivot['Change %'] = ((pivot[2025] - pivot[2024]) / pivot[2024] * 100).replace([np.inf, -np.inf], np.nan)
                pivot = pivot.sort_values('Change %', ascending=False)
                
                pivot.to_excel(writer, sheet_name=f'{col} Longitudinal', index=False)
                self.format_worksheet(writer.sheets[f'{col} Longitudinal'], pivot, 'Change %', 'change')
            
            # Statistical analysis
            stats_cols = [f'{col} per Pax' for col in self.cost_columns + ['COGS']]
            stats = agg_data.groupby(['Event', 'Event ID', 'Season'])[stats_cols].agg(['mean', 'std', 'max', 'min']).reset_index()
            
            for col in stats_cols:
                stat_df = stats[[('Event', ''), ('Event ID', ''), ('Season', ''), (col, 'mean'), (col, 'std'), (col, 'max'), (col, 'min')]]
                stat_df.columns = ['Event', 'Event ID', 'Season', 'Mean', 'Std', 'Max', 'Min']
                stat_df.to_excel(writer, sheet_name=f'{col} Stats', index=False)
                self.format_worksheet(writer.sheets[f'{col} Stats'], stat_df, 'Std', 'std')
    
    def per_vendor_cogs_analysis(self, vendor_data):
        """Perform Per Vendor COGS analysis"""
        # Add per pax columns
        vendor_data = self.add_per_pax_columns(vendor_data)
        
        with pd.ExcelWriter(os.path.join(self.OUTPUT_DIR, 'per_vendor_cogs_analysis.xlsx'), engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # Longitudinal analysis
            for col in self.cost_columns + ['COGS']:
                pivot = vendor_data.pivot_table(
                    values=f'{col} per Pax',
                    index=['Event', 'Event ID', 'Season'],
                    columns='Year'
                ).reset_index()
                
                pivot['Change %'] = ((pivot[2025] - pivot[2024]) / pivot[2024] * 100).replace([np.inf, -np.inf], np.nan)
                pivot = pivot.sort_values('Change %', ascending=False)
                
                pivot.to_excel(writer, sheet_name=f'{col} Longitudinal', index=False)
                self.format_worksheet(writer.sheets[f'{col} Longitudinal'], pivot, 'Change %', 'change')
            
            # Cross-sectional analysis
            cross_data = vendor_data.groupby(['Vendor', 'Season', 'Year'])[self.cost_columns + ['COGS', 'Actual Pax']].sum().reset_index()
            cross_data = self.add_per_pax_columns(cross_data)
            
            for col in self.cost_columns + ['COGS']:
                pivot = cross_data.pivot_table(
                    values=f'{col} per Pax',
                    index=['Vendor'],
                    columns=['Year', 'Season']
                ).reset_index()
                
                pivot.to_excel(writer, sheet_name=f'{col} Cross-sectional', index=False)
                self.format_worksheet(writer.sheets[f'{col} Cross-sectional'], pivot, f'{col} per Pax', 'minmax')
            
            # Statistical analysis - Longitudinal
            stats_cols = [f'{col} per Pax' for col in self.cost_columns + ['COGS']]
            long_stats = vendor_data.groupby(['Event', 'Event ID', 'Season'])[stats_cols].agg(['mean', 'std', 'max', 'min']).reset_index()
            
            for col in stats_cols:
                stat_df = long_stats[[('Event', ''), ('Event ID', ''), ('Season', ''), (col, 'mean'), (col, 'std'), (col, 'max'), (col, 'min')]]
                stat_df.columns = ['Event', 'Event ID', 'Season', 'Mean', 'Std', 'Max', 'Min']
                stat_df.to_excel(writer, sheet_name=f'{col} Long Stats', index=False)
                self.format_worksheet(writer.sheets[f'{col} Long Stats'], stat_df, 'Std', 'std')
            
            # Statistical analysis - Cross-sectional
            cross_stats = cross_data.groupby(['Year', 'Season'])[stats_cols].agg(['mean', 'std', 'max', 'min']).reset_index()
            
            for col in stats_cols:
                stat_df = cross_stats[[('Year', ''), ('Season', ''), (col, 'mean'), (col, 'std'), (col, 'max'), (col, 'min')]]
                stat_df.columns = ['Year', 'Season', 'Mean', 'Std', 'Max', 'Min']
                stat_df.to_excel(writer, sheet_name=f'{col} Cross Stats', index=False)
                self.format_worksheet(writer.sheets[f'{col} Cross Stats'], stat_df, 'Std', 'std')
    
    def run_analysis(self):
        """Run complete COGS analysis"""
        vendor_data, stage_id_data = self.prepare_merged_data()
        self.per_pax_cogs_analysis(stage_id_data)
        self.per_vendor_cogs_analysis(vendor_data)

if __name__ == "__main__":
    analysis = COGSAnalysis()
    analysis.run_analysis()
