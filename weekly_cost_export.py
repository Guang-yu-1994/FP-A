import os
import pandas as pd
from datetime import datetime, timedelta
import logging
import numpy as np

class WeeklyCostExport:
    """
    Weekly Cost Export processing class
    Used to process cost export data, aggregate weekly and generate pivot tables
    """

    def __init__(self):
        # Set base directory and paths
        self.BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        self.INPUT_DIR = os.path.join(self.BASE_DIR, 'Revenue Inputs')
        self.OUTPUT_DIR = os.path.join(self.BASE_DIR, 'Revenue Outputs')
        # Set public database path to user-specified location
        self.Public_DIR = r"C:\City Experience\Public Data Base" # User has moved file to this directory

        # Set up logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        # Ensure output directory and public database directory exist
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)
        os.makedirs(self.Public_DIR, exist_ok=True) # Ensure public directory exists

    def get_start_of_week(self, date):
        """
        Get the Monday date of the week for a given date

        Args:
            date: date object

        Returns:
            Monday date
        """
        if pd.isna(date):
            return None

        # Ensure it is a datetime object
        if isinstance(date, str):
            date = pd.to_datetime(date)

        # Calculate Monday date (weekday() returns 0-6, 0 is Monday)
        days_since_monday = date.weekday()
        start_of_week = date - timedelta(days=days_since_monday)
        return start_of_week.date()

    def load_cost_export_data(self, read_from_excel: bool = True):
        """
        Reads Cost Export data, optionally from Excel or CSV.
        If reading from Excel, it will also be saved in CSV format.

        Args:
            read_from_excel (bool): If True, read from Excel and save as CSV;
                                    If False, read directly from CSV.

        Returns:
            DataFrame: Original data
        """
        excel_filename = 'Costs Export.xlsx'
        excel_sheet_name = 'Cost Export'
        csv_filename = 'AP Prediction_Cost_Export.csv' # Define CSV filename

        excel_file_path = os.path.join(self.Public_DIR, excel_filename)
        csv_file_path = os.path.join(self.Public_DIR, csv_filename)

        df = None
        try:
            if read_from_excel:
                self.logger.info(f"Choosing to re-read from Excel. Reading file: {excel_file_path}")
                # Ensure Excel file exists
                if not os.path.exists(excel_file_path):
                    raise FileNotFoundError(f"Excel file not found: {excel_file_path}")

                df = pd.read_excel(excel_file_path, sheet_name=excel_sheet_name)
                self.logger.info(f"Successfully read data from Excel, total {len(df)} rows")

                # After reading, output in CSV format in the same folder
                self.logger.info(f"Saving data in CSV format: {csv_file_path}")
                df.to_csv(csv_file_path, index=False, encoding='utf-8')
                self.logger.info("CSV file saved successfully.")
            else:
                self.logger.info(f"Choosing to read from CSV. Reading file: {csv_file_path}")
                # Ensure CSV file exists
                if not os.path.exists(csv_file_path):
                    raise FileNotFoundError(f"CSV file not found: {csv_file_path}. Please run in Excel read mode first.")

                df = pd.read_csv(csv_file_path, encoding='utf-8')
                self.logger.info(f"Successfully read data from CSV, total {len(df)} rows")

            return df

        except FileNotFoundError as e:
            self.logger.error(f"File operation error: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Error reading or saving file: {str(e)}")
            raise

    def identify_numeric_columns(self, df):
        """
        Identifies numeric columns in a DataFrame

        Args:
            df: DataFrame

        Returns:
            list: List of numeric column names
        """
        numeric_columns = []

        # Define non-data columns that do not need to be checked
        non_numeric_cols_to_skip = ['StartOfWeek', 'Event Date', 'Event ID', 'Event', 'Currency', 'AP or CF']

        for col in df.columns:
            # Skip non-data columns
            if col in non_numeric_cols_to_skip:
                continue

            # Attempt to convert column to numeric type
            try:
                # errors='coerce' will change values that cannot be converted to NaT/NaN
                temp_series = pd.to_numeric(df[col], errors='coerce')
                # If the number of non-null values after conversion exceeds 50% of the total, it is considered a numeric column
                if temp_series.notna().sum() / len(temp_series) > 0.5:
                    numeric_columns.append(col)
            except:
                continue

        self.logger.info(f"Identified numeric columns: {numeric_columns}")
        return numeric_columns

    def process_weekly_aggregation(self, df):
        """
        Processes weekly aggregation

        Args:
            df: Original DataFrame

        Returns:
            tuple: (Original aggregated data, VCX aggregated data)
        """
        try:
            # 1. Ensure Event Date column exists and is in date format
            if 'Event Date' not in df.columns:
                raise ValueError("Missing 'Event Date' column in data")

            df['Event Date'] = pd.to_datetime(df['Event Date'], errors='coerce')

            # 2. Add StartOfWeek column
            df['StartOfWeek'] = df['Event Date'].apply(self.get_start_of_week)

            self.logger.info("StartOfWeek column added")

            # 3. Check for existence of required columns
            # Now also include 'Guide&Coord' (updated name)
            required_columns_for_pivot = ['StartOfWeek', 'Event ID', 'Event', 'Currency', 'AP or CF',
                                          'COGS exc Guide&Coord', 'Guide&Coord'] # Updated column name
            missing_columns = [col for col in required_columns_for_pivot if col not in df.columns]

            if missing_columns:
                raise ValueError(f"Missing the following columns in data, unable to generate Pivot Table: {missing_columns}")

            # 4. Drop rows where StartOfWeek is empty
            df_clean = df.dropna(subset=['StartOfWeek']).copy()

            # 5. Ensure COGS exc Guide&Coord and Guide&Coord are numeric types
            df_clean['COGS exc Guide&Coord'] = pd.to_numeric(df_clean['COGS exc Guide&Coord'], errors='coerce')
            df_clean['Guide&Coord'] = pd.to_numeric(df_clean['Guide&Coord'], errors='coerce') # Updated column name and ensure numeric

            # 6. Original aggregation (keeping original functionality, for Pivot Table)
            groupby_columns_for_pivot = ['StartOfWeek', 'Event ID', 'Event', 'Currency', 'AP or CF']

            aggregated_df = df_clean.groupby(groupby_columns_for_pivot).agg({
                'COGS exc Guide&Coord': 'sum',
                'Guide&Coord': 'sum'  # Also aggregate Guide&Coord (updated name)
            }).reset_index()

            self.logger.info(f"Original aggregation completed, total {len(aggregated_df)} rows")

            # 7. New: Identify all numeric columns and perform VCX aggregation
            numeric_columns = self.identify_numeric_columns(df_clean)

            if numeric_columns:
                # Ensure all numeric columns are of numeric type
                for col in numeric_columns:
                    df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0)

                # Create aggregation dictionary
                agg_dict = {col: 'sum' for col in numeric_columns}

                # --- Main modification point ---
                # When aggregating VCX data, do not use 'AP or CF' column
                vcx_groupby_columns = ['StartOfWeek', 'Event ID', 'Event', 'Currency']
                vcx_aggregated = df_clean.groupby(vcx_groupby_columns).agg(agg_dict).reset_index()

                # Add VCX prefix to numeric column names
                column_mapping = {col: f'VCX_{col}' for col in numeric_columns}
                vcx_aggregated = vcx_aggregated.rename(columns=column_mapping)

                self.logger.info(f"VCX aggregation completed, total {len(vcx_aggregated)} rows, aggregated {len(numeric_columns)} numeric columns")

            else:
                self.logger.warning("No aggregatable numeric columns identified")
                vcx_aggregated = None

            return aggregated_df, vcx_aggregated

        except Exception as e:
            self.logger.error(f"Error processing weekly aggregation: {str(e)}")
            raise

    def create_pivot_table(self, df):
        """
        Creates a pivot table, handling Guide&Coord merging and column renaming.

        Args:
            df: Aggregated data

        Returns:
            DataFrame: Pivot table
        """
        try:
            # Create pivot table for 'COGS exc Guide&Coord'
            pivot_cogs = df.pivot_table(
                index=['StartOfWeek', 'Event ID', 'Event', 'Currency'],
                columns='AP or CF',
                values='COGS exc Guide&Coord',
                aggfunc='sum',
                fill_value=0
            ).reset_index()

            # Rename columns for COGS exc Guide&Coord, assuming 'AP' and 'CF' are output directly
            # These are the columns for COGS exc Guide&Coord AP and COGS exc Guide&Coord CF
            pivot_cogs.rename(columns={'AP': 'AP', 'CF': 'CF'}, inplace=True)

            # Add new column 'COGS exc Guide&Coord' by summing 'AP' and 'CF'
            if 'AP' in pivot_cogs.columns and 'CF' in pivot_cogs.columns:
                pivot_cogs['COGS exc Guide&Coord'] = pivot_cogs['AP'] + pivot_cogs['CF']
                # Fill any NaN in the new sum column with 0
                pivot_cogs['COGS exc Guide&Coord'].fillna(0, inplace=True)
            else:
                self.logger.warning("Columns 'AP' or 'CF' for 'COGS exc Guide&Coord' not found. 'COGS exc Guide&Coord' sum column not created.")
                if 'COGS exc Guide&Coord' not in pivot_cogs.columns:
                    pivot_cogs['COGS exc Guide&Coord'] = 0 # Ensure column exists even if sum not possible

            # Create pivot table for 'Guide&Coord' (updated name)
            pivot_guide_coord = df.pivot_table(
                index=['StartOfWeek', 'Event ID', 'Event', 'Currency'],
                columns='AP or CF',
                values='Guide&Coord', # Updated column name
                aggfunc='sum',
                fill_value=0
            ).reset_index()

            # Merge 'Guide&Coord AP' and 'Guide&Coord CF' into a single 'Guide&Coord' column
            if 'AP' in pivot_guide_coord.columns and 'CF' in pivot_guide_coord.columns:
                pivot_guide_coord['Guide&Coord'] = pivot_guide_coord['AP'] + pivot_guide_coord['CF'] # Updated column name
                # Drop the original AP and CF columns for Guide&Coord after merging
                pivot_guide_coord.drop(columns=['AP', 'CF'], inplace=True)
            else:
                self.logger.warning("Columns 'AP' or 'CF' for 'Guide&Coord' not found. 'Guide&Coord' sum column not created from AP/CF.")
                if 'Guide&Coord' not in pivot_guide_coord.columns: # Ensure column exists if it wasn't created by sum
                    pivot_guide_coord['Guide&Coord'] = 0 # Default to 0 if not present after aggregation


            # Merge the two pivot tables
            # Select relevant columns from pivot_guide_coord to merge (only the combined 'Guide&Coord')
            final_pivot_df = pd.merge(
                pivot_cogs,
                pivot_guide_coord[['StartOfWeek', 'Event ID', 'Event', 'Currency', 'Guide&Coord']], # Only merged Guide&Coord
                on=['StartOfWeek', 'Event ID', 'Event', 'Currency'],
                how='left'
            )

            # Fill any NaN values that resulted from the merge for 'Guide&Coord' with 0
            if 'Guide&Coord' in final_pivot_df.columns:
                final_pivot_df['Guide&Coord'].fillna(0, inplace=True)

            # Reset column names index
            final_pivot_df.columns.name = None

            self.logger.info(f"Pivot table created, shape: {final_pivot_df.shape}")
            self.logger.info(f"Pivot table column names: {list(final_pivot_df.columns)}")

            return final_pivot_df

        except Exception as e:
            self.logger.error(f"Error creating pivot table: {str(e)}")
            raise

    def save_results(self, pivot_df, vcx_df=None, filename='weekly_cost_export_result.xlsx'):
        """
        Saves results to file

        Args:
            pivot_df: Pivot table result
            vcx_df: VCX aggregation result
            filename: File name
        """
        try:
            output_path = os.path.join(self.OUTPUT_DIR, filename)

            # Use ExcelWriter to save multiple sheets
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Save final pivot table result
                pivot_df.to_excel(writer, sheet_name='Pivot_Result', index=False)
                self.logger.info("Pivot table result saved to 'Pivot_Result' sheet")

                # Save VCX aggregation result (if it exists)
                if vcx_df is not None:
                    vcx_df.to_excel(writer, sheet_name='VCX_Aggregation', index=False)
                    self.logger.info("VCX aggregation result saved to 'VCX_Aggregation' sheet")

            self.logger.info(f"Results saved to: {output_path}")

        except Exception as e:
            self.logger.error(f"Error saving file: {str(e)}")
            raise

    def run_weekly_cost_export(self, read_from_excel: bool = True):
        """
        Executes the complete weekly cost export process, and provides the option to re-read from Excel.

        Args:
            read_from_excel (bool): If True, read from Excel and save as CSV;
                                    If False, read directly from CSV.

        Returns:
            tuple: (Pivot table result, VCX aggregation result)
        """
        try:
            self.logger.info("Starting Weekly Cost Export process...")

            # 1. Read data (based on read_from_excel parameter)
            raw_data = self.load_cost_export_data(read_from_excel=read_from_excel)

            # 2. Process weekly aggregation (including VCX aggregation)
            aggregated_data, vcx_data = self.process_weekly_aggregation(raw_data)

            # 3. Create pivot table (maintaining original functionality)
            pivot_result = self.create_pivot_table(aggregated_data)

            # 4. Save results (including VCX data)
            self.save_results(pivot_result, vcx_data)

            self.logger.info("Weekly Cost Export process completed!")

            return pivot_result, vcx_data

        except Exception as e:
            self.logger.error(f"Error executing process: {str(e)}")
            raise


# Example usage
if __name__ == "__main__":
    exporter = WeeklyCostExport()

    # Optional parameter: whether to re-read from Excel
    # User can modify True/False here to control behavior
    # True: Read from Excel and generate CSV
    # False: Read directly from already generated CSV
    should_read_excel = False # Default to True, will read from Excel and generate CSV on first run

    try:
        pivot_result, vcx_result = exporter.run_weekly_cost_export(read_from_excel=should_read_excel)
        print("Processing complete!")

        if pivot_result is not None:
            print(f"Pivot table result shape: {pivot_result.shape}")
            print("First 5 rows of pivot table preview:")
            print(pivot_result.head())

        if vcx_result is not None:
            print(f"\nVCX aggregation result shape: {vcx_result.shape}")
            print("First 5 rows of VCX aggregation preview:")
            print(vcx_result.head())
            print("VCX aggregation column names:")
            print(list(vcx_result.columns))
        else:
            print("\nNo VCX aggregation result generated")

    except Exception as e:
        print(f"Execution error: {str(e)}")
