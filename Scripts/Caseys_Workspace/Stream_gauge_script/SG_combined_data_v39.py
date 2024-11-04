import os
import pandas as pd
import logging
import re
from openpyxl import load_workbook

# Setup logging to both console and file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("data_processing.log"), logging.StreamHandler()]
)

# Standardized headers and mappings for column alignment
standard_headers = [
    'Date/Time', 'WTlvl_Avg', 'Twt_F_Avg', 'BattVolt_Avg', 'BattVolt_Min',
    'Tpanel_Avg', 'TCair_Avg', 'RHenc', 'RF_Tot (mm)'
]

header_mapping = {
    'Date': 'Date/Time',
    'Time': 'Date/Time',
    'Date Time, GMT-11:00': 'Date/Time',
    'Abs Pres, psi': 'WTlvl_Avg',
    'Temp, °F': 'Twt_F_Avg',
    'Pressure': 'WTlvl_Avg',
    'Temp F': 'Twt_F_Avg',
    'BattVolt_Avg': 'BattVolt_Avg',
    'BattVolt_Min': 'BattVolt_Min',
    'Tpanel_Avg': 'Tpanel_Avg',
    'TCair_Avg': 'TCair_Avg',
    'RHenc': 'RHenc',
    'RF_Tot (mm)': 'RF_Tot (mm)',
}

# List of filenames to ignore
ignore_files = [
    'Nuuuli_4.1.1-2020.12.18.csv', 'Nuuuli_4.1.1-2022.1.6.csv',
    'Nuuuli_4.1.1-2022.10.19.csv', 'Nuuuli_4.1.1-2022.11.17.csv',
    'Nuuuli_4.1.1-2022.12.14.csv', 'Nuuuli_4.1.1-2022.2.8.csv',
    'Nuuuli_4.1.1-2022.4.22.csv', 'Nuuuli_4.1.1-2022.5.16.csv',
    'Nuuuli_4.1.1-2022.6.16.csv', 'Nuuuli_4.1.1-2022.7.15.csv',
    'Nuuuli_4.1.1-2022.8.15.csv', 'Nuuuli_4.1.1-2022.9.15.csv',
    'Nuuuli_ALL_SG_data.xlsx'
]

# Function to map columns based on header_mapping
def map_columns(df):
    logging.info(f"Mapping columns for file with headers: {df.columns.tolist()}")
    df = df.rename(columns=header_mapping)
    df = df.loc[:, ~df.columns.duplicated()]
    for header in standard_headers:
        if header not in df.columns:
            df[header] = None
    logging.info(f"Final mapped columns: {df.columns.tolist()}")
    return df

# Function to clean CSV files
def clean_csv_data(file_path):
    logging.info(f"Processing CSV file: {file_path}")
    df = pd.read_csv(file_path)
    return map_columns(df)

# Function to clean Excel files
def clean_xlsx_data(file_path):
    logging.info(f"Processing XLSX file: {file_path}")
    xl = pd.ExcelFile(file_path)
    logging.info(f"Available sheets: {xl.sheet_names}")
    if 'PT data' in xl.sheet_names:
        df = xl.parse('PT data')
    else:
        for sheet in xl.sheet_names:
            df = xl.parse(sheet)
            if not df.empty:
                logging.info(f"Using sheet: {sheet}")
                break
    return map_columns(df)

# Function to process files based on type
def process_file(file_path):
    logging.info(f"Processing file: {file_path}")
    if file_path.endswith('.csv'):
        return clean_csv_data(file_path)
    elif file_path.endswith('.xlsx'):
        return clean_xlsx_data(file_path)
    else:
        logging.warning(f"Unsupported file format: {file_path}")
        return None

# Function to combine and clean data for the specified station
def process_multiple_files_to_single_sheet(input_directory, output_file, start_date, end_date):
    all_data = []
    logging.info("Starting the script.")
    print("Starting the script...")

    for root, dirs, files in os.walk(input_directory):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            if file_name in ignore_files:
                logging.info(f"Skipping ignored file: {file_name}")
                continue

            extracted_date = extract_date_from_filename(file_name)
            if extracted_date and start_date <= extracted_date <= end_date:
                df_cleaned = process_file(file_path)
                if df_cleaned is not None:
                    all_data.append(df_cleaned)

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        logging.info(f"Saving data to {output_file}")
        combined_df.to_excel(output_file, index=False, engine='openpyxl')
        logging.info(f"Data successfully saved to {output_file}")
        print(f"Data saved to: {output_file}")
    else:
        logging.info("No valid data found.")
        print("No valid data found.")
    print("Script completed.")

# Date extraction from filename using regex
def extract_date_from_filename(file_name):
    try:
        date_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', file_name)
        if date_match:
            month, day, year = date_match.groups()
            return pd.to_datetime(f"{year}-{month}-{day}", format="%Y-%m-%d")
    except Exception as e:
        logging.error(f"Failed to extract date from filename {file_name}: {e}")
    return None

# Folder matching function
def find_matching_station_folder(base_directory, station_name):
    normalized_input = re.sub(r'[\.\s_]', '', station_name.lower())
    for folder in os.listdir(base_directory):
        normalized_folder = re.sub(r'[\.\s_]', '', folder.lower())
        if normalized_input in normalized_folder:
            return os.path.join(base_directory, folder)
    return None

if __name__ == "__main__":
    station_name = input("Enter the stream gauge name (e.g., 1410_Fagalii): ")
    start_date_str = input("Enter the start date (YYYY-MM-DD): ")
    end_date_str = input("Enter the end date (YYYY-MM-DD): ")
    start_date = pd.to_datetime(start_date_str)
    end_date = pd.to_datetime(end_date_str)
    base_directory = 'C:\\Users\\ctebe\\OneDrive\\Desktop\\SG'
    input_directory = find_matching_station_folder(base_directory, station_name)
    if input_directory:
        output_file = os.path.join(input_directory, f'{station_name}_combined_output_single_sheet.xlsx')
        process_multiple_files_to_single_sheet(input_directory, output_file, start_date, end_date)
    else:
        logging.info("Station folder not found.")
        print("Station folder not found.")

