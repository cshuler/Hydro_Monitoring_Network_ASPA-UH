import pandas as pd
import os
import logging
import re
from datetime import datetime

# Setup logging
logging.basicConfig(
    filename='data_processing.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Standardized header
standardized_header = [
    'Date/Time', 'WTlvl_Avg', 'Twt_F_Avg', 'BattVolt_Avg', 'BattVolt_Min', 
    'Tpanel_Avg', 'TCair_Avg', 'RHenc', 'RF_Tot (mm)'
]

# Function to map columns to the standardized header
def map_columns(df, standardized_header):
    # Create a mapping dictionary based on similar column names
    column_mapping = {}
    for std_col in standardized_header:
        for col in df.columns:
            if std_col.lower() in col.lower():
                column_mapping[col] = std_col
                break
    # Apply the mapping to rename the columns
    df = df.rename(columns=column_mapping)
    
    # Add missing columns
    for std_col in standardized_header:
        if std_col not in df.columns:
            df[std_col] = None  # Add empty columns for missing data
    return df[standardized_header]  # Reorder and return only the standardized columns

# Function to clean and process CSV files
def clean_csv_data(file_path):
    try:
        print(f"Processing CSV file: {file_path}")
        df = pd.read_csv(file_path, delimiter=',')
        df = map_columns(df, standardized_header)  # Apply column mapping
        print(f"CSV data: {df.head()}")  # Print first few rows for debugging
        return df
    except Exception as e:
        logging.error(f"Failed to process CSV {file_path}: {e}")
        return None

# Function to clean and process XLSX files
def clean_xlsx_data(file_path):
    try:
        print(f"Processing XLSX file: {file_path}")
        xl = pd.ExcelFile(file_path)
        print(f"Available sheets: {xl.sheet_names}")  # Debug to print sheet names

        # Try to find a valid sheet with data, starting with the first non-empty sheet
        for sheet in xl.sheet_names:
            df = pd.read_excel(file_path, sheet_name=sheet)
            if not df.empty:
                print(f"Using sheet: {sheet}")
                break
        
        df = map_columns(df, standardized_header)  # Apply column mapping
        print(f"XLSX data: {df.head()}")  # Print first few rows for debugging
        return df
    except Exception as e:
        logging.error(f"Failed to process XLSX {file_path}: {e}")
        return None

# Function to process files
def process_file(file_path):
    print(f"Processing file: {file_path}")
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
    print("Starting the script.")

    # Walk through directory and process files
    for root, dirs, files in os.walk(input_directory):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            if file_name.endswith('.csv') or file_name.endswith('.xlsx'):
                extracted_date = extract_date_from_filename(file_name)
                if extracted_date and start_date <= extracted_date <= end_date:
                    df_cleaned = process_file(file_path)
                    if df_cleaned is not None:
                        all_data.append(df_cleaned)

    # Combine and save data if available
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"Saving data to {output_file}")
        combined_df.to_excel(output_file, index=False, engine='openpyxl')
        logging.info(f"Data successfully saved to {output_file}")
        print(f"Data saved to: {output_file}")
    else:
        print("No valid data found.")
        logging.info("No valid data found.")

    print("Script completed.")

# Improved date extraction from filename
def extract_date_from_filename(file_name):
    try:
        date_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', file_name)
        if date_match:
            month, day, year = date_match.groups()
            extracted_date = pd.to_datetime(f"{year}-{month}-{day}", format="%Y-%m-%d")
            logging.info(f"Successfully extracted date: {extracted_date}")
            return extracted_date
        else:
            logging.warning(f"Failed to extract date from {file_name}")
            return None
    except Exception as e:
        logging.error(f"Error extracting date from {file_name}: {e}")
        return None

# Main script
if __name__ == "__main__":
    station_name = input("Enter the stream gauge name (e.g., 1410_Fagalii): ")
    start_date_str = input("Enter the start date (YYYY-MM-DD): ")
    end_date_str = input("Enter the end date (YYYY-MM-DD): ")

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    base_directory = 'C:\\Users\\ctebe\\OneDrive\\Desktop\\SG'

    # Normalize station name for matching
    matched_folder = None
    for folder in os.listdir(base_directory):
        if station_name.lower().replace("_", "").replace("-", "").strip() in folder.lower().replace("_", "").replace("-", "").strip():
            matched_folder = folder
            break

    if matched_folder:
        input_directory = os.path.join(base_directory, matched_folder)
        print(f"Using station folder: {input_directory}")
        output_file = os.path.join(base_directory, f'{station_name}_combined_output_single_sheet.xlsx')
        process_multiple_files_to_single_sheet(input_directory, output_file, start_date, end_date)
    else:
        print(f"Station folder for '{station_name}' not found.")
