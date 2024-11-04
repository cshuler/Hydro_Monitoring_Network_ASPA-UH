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

# Function to extract date from the filename
def extract_date_from_filename(file_name):
    # Date extraction logic as per Comparison_code.txt
    try:
        extracted_date = pd.to_datetime(file_name.split('_')[2], format='%m.%d.%Y')
        print(f"Successfully extracted date: {extracted_date}")
        return extracted_date
    except Exception as e:
        print(f"Failed to extract date from {file_name}: {e}")
        return None

# Function to clean and standardize weather data
def clean_weather_data(file_path):
    try:
        if '~$' in file_path:
            print(f"Skipping temporary file: {file_path}")
            return None

        print(f"Processing file: {file_path}")
        xl = pd.ExcelFile(file_path)
        print(xl.sheet_names)

        # Load the data from the appropriate sheet
        if 'PT data' in xl.sheet_names:
            df = pd.read_excel(file_path, sheet_name='PT data')
        elif 'PT data (5min)' in xl.sheet_names:
            df = pd.read_excel(file_path, sheet_name='PT data (5min)')
        elif 'MetData' in xl.sheet_names:
            df = pd.read_excel(file_path, sheet_name='MetData')
        else:
            print(f"No 'PT data', 'PT data (5min)', or 'MetData' sheet found in {file_path}")
            return None

        print(f"File shape: {df.shape}")

        # Map columns to the standard header and fill missing columns with blank values
        df_mapped = map_columns(df)
        df_cleaned = df_mapped.reindex(columns=STANDARD_HEADER, fill_value='')

        # Check if the DataFrame is entirely empty or all NaN
        if df_cleaned.isna().all().all():
            print(f"File {file_path} contains no valid data.")
            return None

        return df_cleaned
    except Exception as e:
        logging.error(f"Failed to process {file_path}: {e}")
        print(f"Error processing {file_path}: {e}")
        return None

# Function to process multiple files and save them to a single Excel sheet
def process_multiple_files_to_single_sheet(input_directory, output_file, start_date, end_date):
    all_data = []

    # Loop through files in the input directory
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

        # Save the combined data to the output Excel file
        try:
            print(f"Saving data to {output_file}")
            combined_df.to_excel(output_file, index=False, engine='openpyxl')
            logging.info(f"Data successfully saved to {output_file}")
        except Exception as e:
            logging.error(f"Failed to save the file {output_file}: {e}")
            print(f"Error saving the file: {e}")
    else:
        print("No valid data found within the specified date range.")

# Example usage: get the station name and date range from user input
station_name = input("Enter the weather station name (e.g., 1311_Poloa): ")
start_date_str = input("Enter the start date (YYYY-MM-DD): ")
end_date_str = input("Enter the end date (YYYY-MM-DD): ")

# Parse the dates
start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

input_directory = fr'C:\Users\ctebe\OneDrive\Desktop\GitHub Repositories\Hydro_Monitoring_Network_ASPA-UH\Final_Historical_Data\Weather_Stations\{station_name}'
output_file = fr'C:\Users\ctebe\OneDrive\Desktop\GitHub Repositories\Hydro_Monitoring_Network_ASPA-UH\Final_Historical_Data\{station_name}_combined_output_single_sheet.xlsx'

process_multiple_files_to_single_sheet(input_directory, output_file, start_date, end_date)
