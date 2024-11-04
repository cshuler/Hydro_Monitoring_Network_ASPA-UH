import pandas as pd
import os
import re
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    filename='data_processing.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Standardized headers (desired final column names)
standard_headers = [
    'Date/Time', 'WTlvl_Avg', 'Twt_F_Avg', 'BattVolt_Avg', 'BattVolt_Min', 
    'Tpanel_Avg', 'TCair_Avg', 'RHenc', 'RF_Tot (mm)'
]

# Header variations mapped to standardized headers
header_mapping = {
    'Date': 'Date/Time',
    'Time': 'Date/Time',
    'Date Time, GMT-11:00': 'Date/Time',
    'Abs Pres (psi) c:1 2': 'WTlvl_Avg',
    'Abs Pres, psi': 'WTlvl_Avg',
    'Pressure': 'WTlvl_Avg',
    'Temp (°F) c:2': 'Twt_F_Avg',
    'Temp, °F': 'Twt_F_Avg',
    'Temp F': 'Twt_F_Avg',
    'BattVolt_Avg': 'BattVolt_Avg',
    'BattVolt_Min': 'BattVolt_Min',
    'Tpanel_Avg': 'Tpanel_Avg',
    'TCair_Avg': 'TCair_Avg',
    'RHenc': 'RHenc',
    'RF_Tot (mm)': 'RF_Tot (mm)',
    'Coupler Detached': None,
    'Coupler Attached': None,
    'Host Connected': None,
    'Stopped': None,
    'End Of File': None,
    'Plot Title': None
}

# List of filenames to ignore
ignore_files = [
    'Nuuuli_4.1.1-2020.12.18.csv', 'Nuuuli_4.1.1-2022.1.6.csv',
    'Nuuuli_4.1.1-2022.10.19.csv', 'Nuuuli_4.1.1-2022.11.17.csv',
    'Nuuuli_4.1.1-2022.12.14.csv', 'Nuuuli_4.1.1-2022.2.8.csv',
    'Nuuuli_4.1.1-2022.4.22.csv', 'Nuuuli_4.1.1-2022.5.16.csv',
    'Nuuuli_4.1.1-2022.6.16.csv', 'Nuuuli_4.1.1-2022.7.15.csv',
    'Nuuuli_4.1.1-2022.8.15.csv', 'Nuuuli_4.1.1-2022.9.15.csv',
    'Nuuuli_4.1.1-2023.1.17.csv', 'Nuuuli_4.1.1-2023.10.16.csv',
    'Nuuuli_4.1.1-2023.12.15.csv', 'Nuuuli_4.1.1-2023.2.15.csv',
    'Nuuuli_4.1.1-2023.3.17.csv', 'Nuuuli_4.1.1-2023.4.14.csv',
    'Nuuuli_4.1.1-2023.5.15.csv', 'Nuuuli_4.1.1-2023.6.15.csv',
    'Nuuuli_4.1.1-2023.7.14.csv', 'Nuuuli_4.1.1-2023.8.16.csv',
    'Nuuuli_4.1.1-2023.9.15.csv', 'Nuuuli_ALL_SG_data.xlsx'
]

# Date extraction from filename using regex
def extract_date_from_filename(file_name):
    try:
        date_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', file_name)
        if date_match:
            month, day, year = date_match.groups()
            extracted_date = pd.to_datetime(f"{year}-{month}-{day}", format="%Y-%m-%d")
            logging.info(f"Extracted date {extracted_date} from file: {file_name}")
            return extracted_date
    except Exception as e:
        logging.error(f"Failed to extract date from filename {file_name}: {e}")
    return None

# Function to map columns to the standardized header
def map_columns(df):
    print(f"Mapping columns for file with headers: {df.columns.tolist()}")
    
    # Rename columns based on header mapping
    new_columns = {}
    for col in df.columns:
        if col in header_mapping and header_mapping[col]:
            new_columns[col] = header_mapping[col]
    df = df.rename(columns=new_columns)

    # Add missing columns with blank ('') values
    for std_col in standard_headers:
        if std_col not in df.columns:
            df[std_col] = ''  # Add a column with blank values if it's missing

    print(f"Final mapped columns: {df.columns.tolist()}")
    
    # Return only the standardized columns in the expected order
    return df[standard_headers]

# Function to clean and process CSV files
def clean_csv_data(file_path):
    try:
        logging.info(f"Processing CSV file: {file_path}")
        print(f"Processing CSV file: {file_path}")
        df = pd.read_csv(file_path)
        df = map_columns(df)  # Apply column mapping
        print(f"Processed CSV data preview:\n{df.head()}")
        return df
    except Exception as e:
        logging.error(f"Failed to process CSV {file_path}: {e}")
        print(f"Error processing CSV file: {file_path}")
        return None

# Function to clean and process XLSX files
def clean_xlsx_data(file_path):
    try:
        logging.info(f"Processing XLSX file: {file_path}")
        print(f"Processing XLSX file: {file_path}")
        xl = pd.ExcelFile(file_path)
        print(f"Available sheets: {xl.sheet_names}")  # Debug to print sheet names

        # Use "PT data" sheet if it exists, otherwise select the first non-empty sheet
        if 'PT data' in xl.sheet_names:
            df = pd.read_excel(file_path, sheet_name='PT data')
        else:
            for sheet in xl.sheet_names:
                df = pd.read_excel(file_path, sheet_name=sheet)
                if not df.empty:
                    print(f"Using sheet: {sheet}")
                    break
        
        df = map_columns(df)  # Apply column mapping
        print(f"Processed XLSX data preview:\n{df.head()}")
        return df
    except Exception as e:
        logging.error(f"Failed to process XLSX {file_path}: {e}")
        print(f"Error processing XLSX file: {file_path}")
        return None

# Function to process files
def process_file(file_path):
    logging.info(f"Processing file: {file_path}")
    print(f"Processing file: {file_path}")
    if file_path.endswith('.csv'):
        df = clean_csv_data(file_path)
    elif file_path.endswith('.xlsx'):
        df = clean_xlsx_data(file_path)
    else:
        logging.warning(f"Unsupported file format: {file_path}")
        return None
    
    # Reset the index to avoid index conflicts
    if df is not None:
        df = df.reset_index(drop=True)
    return df

# Folder matching logic to find station folder
def find_matching_station_folder(base_directory, station_name):
    normalized_input = re.sub(r'[\.\s_]', '', station_name.lower())
    for folder in os.listdir(base_directory):
        normalized_folder = re.sub(r'[\.\s_]', '', folder.lower())
        if normalized_input in normalized_folder:
            return os.path.join
