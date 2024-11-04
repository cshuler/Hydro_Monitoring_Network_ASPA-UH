import pandas as pd
import os
import logging
from datetime import datetime
import re

# Setup logging
logging.basicConfig(
    filename='data_processing.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Define the standard header
STANDARD_HEADER = [
    'Date/Time', 'Swin_Avg (W/m²)', 'Thermocouple C', 'RH_Avg Percent',
    'VP_Avg (kPa)', 'VPsat_Avg (kPa)', 'VPD_Avg (kPa)', 'WS_Avg (m/s)',
    'WSrs_Avg (m/s)', 'WDuv_Avg (degrees)', 'WDrs_Avg (degrees)',
    'WD_StdY (degrees)', 'WD_StdCS (degrees)', 'SM_1_Avg (m3/m3)', 'Tsoil_1 C', 'RF_Tot (mm)'
]

# Variable mapping with possible variations
variable_mapping = {
    'Date/Time': ['Date/Time', 'Datetime', 'Timestamp'],
    'Swin_Avg (W/m²)': ['Swin_Avg (W/m²)', 'Irradiance (AVG)', 'Irradiance (Tot)', 'Swin_Avg'],
    'Thermocouple C': ['Thermocouple C', 'Temp C', 'Tair_ Avg C', 'Tair_Avg C'],
    'RH_Avg Percent': ['RH_Avg Percent', 'RH Percent', 'RH'],
    'VP_Avg (kPa)': ['VP_Avg (kPa)', 'Vapor Pressure Avg'],
    'VPsat_Avg (kPa)': ['VPsat_Avg (kPa)', 'VPsat_Avg'],
    'VPD_Avg (kPa)': ['VPD_Avg (kPa)', 'VPD_Avg'],
    'WS_Avg (m/s)': ['WS_Avg (m/s)', 'Wind Speed (MPH)', 'Wind Speed'],
    'WSrs_Avg (m/s)': ['WSrs_Avg (m/s)', 'Wind Vector SD (Deg)', 'Wind Vector SD'],
    'WDuv_Avg (degrees)': ['WDuv_Avg (degrees)', 'Wind Direction (Deg)', 'Wind Direction'],
    'WDrs_Avg (degrees)': ['WDrs_Avg (degrees)'],
    'WD_StdY (degrees)': ['WD_StdY (degrees)', 'WD_StdY'],
    'WD_StdCS (degrees)': ['WD_StdCS (degrees)', 'WD Std Dev (Deg)', 'WD Std Dev'],
    'SM_1_Avg (m3/m3)': ['SM_1_Avg (m3/m3)', 'Soil Moisture'],
    'Tsoil_1 C': ['Tsoil_1 C', 'Tsoil C'],
    'RF_Tot (mm)': ['RF_Tot (mm)', 'Precipitation', 'Rainfall']
}

# Function to map columns to the standard header
def map_columns(df):
    new_columns = {}
    for standard_col, variations in variable_mapping.items():
        for col in variations:
            if col in df.columns:
                new_columns[col] = standard_col
                break
    return df.rename(columns=new_columns)

# Improved function to extract date from filename using regular expressions
def extract_date_from_filename(file_name):
    date_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', file_name)
    if date_match:
        month, day, year = date_match.groups()
        extracted_date = pd.to_datetime(f"{year}-{month}-{day}", format="%Y-%m-%d")
        logging.info(f"Successfully extracted date: {extracted_date}")
        return extracted_date
    else:
        logging.error(f"Failed to extract date from {file_name}")
        return None

# Function to clean and standardize weather data
def clean_weather_data(file_path):
    try:
        if '~$' in file_path:
            logging.info(f"Skipping temporary file: {file_path}")
            return None

        logging.info(f"Processing file: {file_path}")
        xl = pd.ExcelFile(file_path)

        if 'PT data' in xl.sheet_names:
            df = pd.read_excel(file_path, sheet_name='PT data')
        elif 'PT data (5min)' in xl.sheet_names:
            df = pd.read_excel(file_path, sheet_name='PT data (5min)')
        elif 'MetData' in xl.sheet_names:
            df = pd.read_excel(file_path, sheet_name='MetData')
        else:
            logging.warning(f"No relevant sheet found in {file_path}")
            return None

        df_mapped = map_columns(df)
        df_cleaned = df_mapped.reindex(columns=STANDARD_HEADER, fill_value='')
        return df_cleaned if not df_cleaned.isna().all().all() else None
    except Exception as e:
        logging.error(f"Failed to process {file_path}: {e}")
        return None

# Main function to process and combine files
def process_multiple_files_to_single_sheet(input_directory, output_file, start_date, end_date):
    all_data = []
    for root, dirs, files in os.walk(input_directory):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            if file_name.endswith('.xlsx'):
                extracted_date = extract_date_from_filename(file_name)
                if extracted_date and start_date <= extracted_date <= end_date:
                    df_cleaned = clean_weather_data(file_path)
                    if df_cleaned is not None:
                        all_data.append(df_cleaned)

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df.to_excel(output_file, index=False, engine='openpyxl')

# Script execution
if __name__ == "__main__":
    station_name = input("Enter the weather station name (e.g., 1311_Poloa): ")
    start_date_str = input("Enter the start date (YYYY-MM-DD): ")
    end_date_str = input("Enter the end date (YYYY-MM-DD): ")

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    input_directory = f'C:/Users/ctebe/OneDrive/Desktop/GitHub Repositories/Hydro_Monitoring_Network_ASPA-UH/Final_Historical_Data/Weather_Stations/{station_name}'
    output_file = f'C:/Users/ctebe/OneDrive/Desktop/GitHub Repositories/Hydro_Monitoring_Network_ASPA-UH/Final_Historical_Data/{station_name}_combined_output_single_sheet.xlsx'

    process_multiple_files_to_single_sheet(input_directory, output_file, start_date, end_date)
