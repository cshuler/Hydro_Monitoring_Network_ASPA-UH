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
    'WD_StdY (degrees)', 'WD_StdCS (degrees)', 'SM_1_Avg (m3/m3)',
    'Tsoil_1 C', 'RF_Tot (mm)'
]

# Column name variations mapping
variable_mapping = {
    'Date/Time': ['Date/Time', 'Datetime', 'Timestamp'],
    'Swin_Avg (W/m²)': ['Swin_Avg (W/m²)', 'Irradiance (AVG)', 'Swin_Avg'],
    'Thermocouple C': ['Thermocouple C', 'Temp C'],
    'RH_Avg Percent': ['RH_Avg Percent', 'RH Percent'],
    'VP_Avg (kPa)': ['VP_Avg (kPa)', 'Vapor Pressure Avg'],
    'VPsat_Avg (kPa)': ['VPsat_Avg (kPa)', 'VPsat_Avg'],
    'VPD_Avg (kPa)': ['VPD_Avg (kPa)', 'VPD_Avg'],
    'WS_Avg (m/s)': ['WS_Avg (m/s)', 'Wind Speed (MPH)'],
    'WSrs_Avg (m/s)': ['WSrs_Avg (m/s)', 'WSrs_Avg'],
    'WDuv_Avg (degrees)': ['WDuv_Avg (degrees)', 'Wind Direction (Deg)'],
    'WDrs_Avg (degrees)': ['WDrs_Avg (degrees)', 'WDrs_Avg'],
    'WD_StdY (degrees)': ['WD_StdY (degrees)', 'WD_StdY'],
    'WD_StdCS (degrees)': ['WD_StdCS (degrees)', 'WD_StdCS'],
    'SM_1_Avg (m3/m3)': ['SM_1_Avg (m3/m3)', 'Soil Moisture'],
    'Tsoil_1 C': ['Tsoil_1 C', 'Tsoil'],
    'RF_Tot (mm)': ['RF_Tot (mm)', 'Precipitation']
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

# Function to clean and standardize weather data
def clean_weather_data(file_path):
    try:
        if '~$' in file_path:
            print(f"Skipping temporary file: {file_path}")
            return None

        print(f"Processing file: {file_path}")
        xl = pd.ExcelFile(file_path)
        print(xl.sheet_names)  # Print all sheet names for debugging
        
        # Load the data from the appropriate sheet
        if 'PT data' in xl.sheet_names:
            df = pd.read_excel(file_path, sheet_name='PT data')
        elif 'PT data (5min)' in xl.sheet_names:
            df = pd.read_excel(file_path, sheet_name='PT data (5min)')
        elif 'MetData' in xl.sheet_names:
            df = pd.read_excel(file_path, sheet_name='MetData')  # Use MetData as a fallback if available
        else:
            print(f"No 'PT data', 'PT data (5min)', or 'MetData' sheet found in {file_path}")
            return None

        print(f"File shape: {df.shape}")

        # Map columns to the standard header and fill missing columns with blank values
        df_mapped = map_columns(df)
        df_cleaned = df_mapped.reindex(columns=STANDARD_HEADER, fill_value='')

        # Check if the DataFrame is entirely empty or all NaN and skip if necessary
        if df_cleaned.isna().all().all():
            print(f"File {file_path} contains no valid data.")
            return None

        print(f"Finished processing: {file_path}")
        return df_cleaned

    except Exception as e:
        logging.error(f"Failed to process file {file_path}: {e}")
        print(f"Error processing {file_path}: {e}")
        return None

# Function to process multiple files in the given weather station directory
def process_multiple_files(input_dir, output_file):
    combined_df = pd.DataFrame()  # Initialize an empty DataFrame

    # Traverse through each subdirectory and process .xlsx files
    for root, dirs, files in os.walk(input_dir):
        for filename in files:
            if filename.endswith(".xlsx") or filename.endswith(".xls"):
                file_path = os.path.join(root, filename)
                df_cleaned = clean_weather_data(file_path)

                if df_cleaned is not None:
                    combined_df = pd.concat([combined_df, df_cleaned], ignore_index=True)
                    logging.info(f"Successfully processed: {file_path}")

    # Write the combined data to the output Excel file
    combined_df.to_excel(output_file, index=False, engine='openpyxl')
    print(f"All files processed and saved into {output_file}")
    logging.info(f"All files processed and saved into {output_file}")

# Main function to select the weather station and specify the output file
def main():
    station_name = input("Enter the weather station name (e.g., 1311_Poloa): ")
    input_directory = f'C:\\Users\\ctebe\\OneDrive\\Desktop\\GitHub Repositories\\Hydro_Monitoring_Network_ASPA-UH\\Final_Historical_Data\\Weather_Stations\\{station_name}'
    output_file = f'{station_name}_combined_output_single_sheet.xlsx'

    print(f"Processing files in directory: {input_directory}")
    process_multiple_files(input_directory, output_file)

# Run the main function when the script is executed
if __name__ == "__main__":
    main()
