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

# Load the standardized header from the file
headers_file_path = 'C:/Users/ctebe/OneDrive/Desktop/GitHub Repositories/Hydro_Monitoring_Network_ASPA-UH/Scripts/Caseys_Workspace/Stream gauge script/Headers.xlsx' # Update the file path accordingly
headers_df = pd.read_excel(headers_file_path)
standardized_header = headers_df.iloc[0].tolist()  # First row of headers file

# Function to map columns to the standardized header
def map_columns(df):
    # Create a mapping between the standardized header and variations in the dataset
    mapping_dict = {
        'Date/Time': ['Date', 'Time', 'Date Time', 'Timestamp'],
        'WTlvl_Avg': ['Abs Pres (psi)', 'Pressure'],
        'Twt_F_Avg': ['Temp', 'Temperature', 'Temp F', 'Temp °F'],
        'BattVolt_Avg': ['Volts', 'Battery Voltage', 'BattVolt_Avg'],
        'BattVolt_Min': ['Min Volts', 'BattVolt_Min'],
        'Tpanel_Avg': ['Panel Temp', 'Tpanel_Avg'],
        'TCair_Avg': ['Air Temp', 'TCair_Avg'],
        'RHenc': ['RHenc', 'Relative Humidity'],
        'RF_Tot (mm)': ['Rainfall', 'RF_Tot (mm)', 'Rainfall (mm)']
    }
    
    new_columns = {}
    for standard_col in standardized_header:
        for col_variation in mapping_dict.get(standard_col, []):
            if col_variation in df.columns:
                new_columns[col_variation] = standard_col
                break
    return df.rename(columns=new_columns)

# Function to clean and process CSV files
def clean_csv_data(file_path):
    try:
        print(f"Processing CSV file: {file_path}")
        df = pd.read_csv(file_path)
        df = map_columns(df)  # Apply column mapping
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

        # Try to find a valid sheet with data
        df = pd.read_excel(file_path, sheet_name=xl.sheet_names[0])  # Default to the first sheet
        df = map_columns(df)  # Apply column mapping
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
    print("Starting the script...")

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
        combined_df.columns = standardized_header  # Ensure the output has the standardized header
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

    # Adjust file paths based on user input
    base_directory = 'C:\\Users\\ctebe\\OneDrive\\Desktop\\SG'
    matched_folder = None

    # Normalize station name for matching
    normalized_station_name = re.sub(r'[^a-zA-Z0-9]', '', station_name.lower())

    # Find matching folder
    for folder in os.listdir(base_directory):
        normalized_folder_name = re.sub(r'[^a-zA-Z0-9]', '', folder.lower())
        if normalized_station_name in normalized_folder_name:
            matched_folder = folder
            break

    if matched_folder:
        input_directory = os.path.join(base_directory, matched_folder)
        print(f"Using station folder: {input_directory}")
    else:
        print(f"Station folder for '{station_name}' not found.")
        exit()

    output_file = os.path.join(base_directory, f'{station_name}_combined_output_single_sheet.xlsx')

    process_multiple_files_to_single_sheet(input_directory, output_file, start_date, end_date)
