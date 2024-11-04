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

# Function to process CSV files with commas as delimiters
def clean_csv_data(file_path):
    try:
        logging.info(f"Processing CSV file: {file_path}")
        df = pd.read_csv(file_path, delimiter=',')
        return df
    except Exception as e:
        logging.error(f"Failed to process CSV {file_path}: {e}")
        return None

# Function to process XLSX files, specifically reading 'PT data' sheet
def clean_xlsx_data(file_path):
    try:
        logging.info(f"Processing XLSX file: {file_path}")
        xl = pd.ExcelFile(file_path)

        if 'PT data' in xl.sheet_names:
            df = pd.read_excel(file_path, sheet_name='PT data')
            return df
        else:
            logging.warning(f"'PT data' sheet not found in {file_path}")
            return None
    except Exception as e:
        logging.error(f"Failed to process XLSX {file_path}: {e}")
        return None

# Function to process files based on extension
def process_file(file_path):
    if file_path.endswith('.csv'):
        return clean_csv_data(file_path)
    elif file_path.endswith('.xlsx'):
        return clean_xlsx_data(file_path)
    else:
        logging.warning(f"Unsupported file format: {file_path}")
        return None

# Function to combine data from multiple files into a single DataFrame
def process_multiple_files(input_directory, start_date, end_date):
    all_data = []
    print("Starting the script...")

    for root, dirs, files in os.walk(input_directory):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            if file_name.endswith('.csv') or file_name.endswith('.xlsx'):
                extracted_date = extract_date_from_filename(file_name)
                if extracted_date and start_date <= extracted_date <= end_date:
                    df_cleaned = process_file(file_path)
                    if df_cleaned is not None:
                        all_data.append(df_cleaned)

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"Processed {len(all_data)} files.")
        return combined_df
    else:
        print("No valid data found.")
        return None

# Function to extract date from filename
def extract_date_from_filename(file_name):
    date_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', file_name)
    if date_match:
        month, day, year = date_match.groups()
        return pd.to_datetime(f"{year}-{month}-{day}", format="%Y-%m-%d")
    return None

# Add the new folder matching logic
def find_station_folder(station_name, base_directory):
    # Normalize station name for matching
    normalized_station_name = re.sub(r'[^a-zA-Z0-9]', '', station_name.lower())

    matched_folder = None
    for folder in os.listdir(base_directory):
        normalized_folder_name = re.sub(r'[^a-zA-Z0-9]', '', folder.lower())  # Remove special characters
        if normalized_station_name in normalized_folder_name:
            matched_folder = folder
            break

    return matched_folder

# Script execution
if __name__ == "__main__":
    station_name = input("Enter the stream gauge name (e.g., 1410_Fagalii): ")
    start_date_str = input("Enter the start date (YYYY-MM-DD): ")
    end_date_str = input("Enter the end date (YYYY-MM-DD): ")

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    base_directory = 'C:\\Users\\ctebe\\OneDrive\\Desktop\\SG'

    # Use the new function to find the correct station folder
    matched_folder = find_station_folder(station_name, base_directory)

    if matched_folder:
        input_directory = os.path.join(base_directory, matched_folder)
        print(f"Using station folder: {input_directory}")
        combined_df = process_multiple_files(input_directory, start_date, end_date)

        if combined_df is not None:
            output_file = os.path.join(base_directory, f'{station_name}_combined_output_single_sheet.xlsx')
            combined_df.to_excel(output_file, index=False, engine='openpyxl')
            print(f"Data saved to: {output_file}")
        else:
            print("No data to save.")
    else:
        print(f"Station folder for '{station_name}' not found.")
