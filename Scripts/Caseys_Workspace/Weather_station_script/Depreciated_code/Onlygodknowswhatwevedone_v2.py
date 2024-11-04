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
    'RF_Tot (mm)': ['RF_Tot (mm)', 'Precipitation', 'Rainfall'],
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
        print(xl.sheet_names)

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

def extract_date_from_filename(filename):
    """Attempt to extract a date from the filename assuming multiple date formats."""
    print(f"Trying to extract date from filename: {filename}")
    date_pattern = r"(\d{1,2})\.(\d{1,2})\.(\d{4})"
    match = re.search(date_pattern, filename)

    if match:
        day, month, year = match.groups()
        date_str = f"{day}.{month}.{year}"
        date_formats = ["%d.%m.%Y", "%m.%d.%Y", "%Y.%m.%d"]

        for date_format in date_formats:
            try:
                date = datetime.strptime(date_str, date_format)
                print(f"Extracted date: {date} using format {date_format}")
                return date
            except ValueError:
                continue

    print(f"Failed to extract date from filename: {filename}")
    return None

def process_multiple_files_to_single_sheet(input_dir, output_file, start_date, end_date):
    print(f"Processing files in directory: {input_dir}")
    combined_df = pd.DataFrame()

    excluded_dirs = ['Master data sheet']

    for root, dirs, files in os.walk(input_dir):
        dirs[:] = [d for d in dirs if d not in excluded_dirs]

        for filename in files:
            if filename.startswith('~$'):
                print(f"Skipping temporary file: {filename}")
                continue

            file_date = extract_date_from_filename(filename)
            if file_date is None or not (start_date <= file_date <= end_date):
                continue

            if filename.endswith(".xlsx") or filename.endswith(".xls"):
                file_path = os.path.join(root, filename)
                df_cleaned = clean_weather_data(file_path)

                if df_cleaned is not None and not df_cleaned.empty:
                    combined_df = pd.concat([combined_df, df_cleaned], ignore_index=True)
                    logging.info(f"Successfully processed: {file_path}")

    if combined_df.empty:
        print("No data was processed.")
    else:
        print("Combined DataFrame shape: ", combined_df.shape)
        print("First few rows of the data: ")
        print(combined_df.head())

        try:
            combined_df.to_excel(output_file, index=False, engine='openpyxl')
            logging.info(f"All files processed and saved into {output_file}")
            print(f"All files processed and saved into {output_file}")
        except Exception as e:
            print(f"Error saving file: {e}")
            logging.error(f"Failed to save file {output_file}: {e}")

# Main function to select the weather station and specify the output file
