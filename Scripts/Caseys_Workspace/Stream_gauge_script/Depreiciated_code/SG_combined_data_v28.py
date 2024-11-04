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

# Load the standardized header from the file (optionally)
headers_file_path = 'C:/Users/ctebe/OneDrive/Desktop/GitHub Repositories/Hydro_Monitoring_Network_ASPA-UH/Scripts/Caseys_Workspace/Stream_gauge_script/Headers.xlsx'
try:
    headers_df = pd.read_excel(headers_file_path)
    standardized_header = headers_df.iloc[0].tolist()  # First row of headers file
except Exception as e:
    logging.error(f"Failed to load headers from Excel file: {e}")
    print("Error: Could not load headers from Excel file. Using fallback header.")
    # Fallback to predefined header if loading fails
    standardized_header = ['Date/Time', 'WTlvl_Avg', 'Twt_F_Avg', 'BattVolt_Avg', 'BattVolt_Min', 'Tpanel_Avg', 'TCair_Avg', 'RHenc', 'RF_Tot (mm)']

# Adjust the column mapping to include only the relevant columns, excluding the ones to be ignored
relevant_columns = {
    'Date': 'Date',
    'Time': 'Time',
    'Abs Pres (psi) c:1 2': 'Abs Pres (psi)',
    'Temp (°F) c:2': 'Temp (°F)'
}

# Columns to be ignored
ignored_columns = ['Coupler Detached', 'Coupler Attached', 'Host Connected', 'Stopped', 'End Of File']

# Code to combine 'Date' and 'Time' columns into a 'Date/Time' column
def combine_date_time_columns(df):
    """
    Combines 'Date' and 'Time' columns into a single 'Date/Time' column.
    Drops the original 'Date' and 'Time' columns after combining.
    """
    if 'Date' in df.columns and 'Time' in df.columns:
        df['Date/Time'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], errors='coerce')  # Combine Date and Time
        df.drop(columns=['Date', 'Time'], inplace=True)  # Remove the original Date and Time columns
    return df

# Function to map columns to the standardized header and remove ignored columns
def map_columns(df):
    print(f"Mapping columns for file with headers: {df.columns.tolist()}")
    new_columns = {}
    for col in relevant_columns.keys():
        if col in df.columns:
            new_columns[col] = relevant_columns[col]

    df = df.rename(columns=new_columns)

    # Drop ignored columns
    df.drop(columns=[col for col in ignored_columns if col in df.columns], inplace=True, errors='ignore')

    # Combine Date and Time columns if present
    df = combine_date_time_columns(df)

    return df

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

# Main script to iterate over files and process
def process_multiple_files_to_single_sheet(input_directory, output_file, start_date, end_date):
    logging.info(f"Processing files from {input_directory}")
    all_data = []

    for root, dirs, files in os.walk(input_directory):
        for file in files:
            file_path = os.path.join(root, file)
            if file.lower().endswith('.csv'):
                df = clean_csv_data(file_path)
            elif file.lower().endswith('.xlsx'):
                df = clean_xlsx_data(file_path)
            else:
                continue

            if df is not None:
                all_data.append(df)

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        # Save combined DataFrame to Excel
        combined_df.to_excel(output_file, index=False)
        print(f"Data saved to: {output_file}")
        print(f"Final combined data preview:\n{combined_df.head()}")
    else:
        logging.warning(f"No data found in directory: {input_directory}")
        print(f"No data found in directory: {input_directory}")

# Function to normalize station names and find the appropriate folder
def normalize_station_name(station_name, base_directory):
    matched_folder = None
    station_name_clean = station_name.lower().replace("_", "").replace("-", "").replace(".", "").strip()

    print("Available folders in base directory:")
    for folder in os.listdir(base_directory):
        print(f"Folder: {folder}")
    
    for folder in os.listdir(base_directory):
        folder_clean = folder.lower().replace("_", "").replace("-", "").replace(".", "").strip()
        if station_name_clean in folder_clean:
            matched_folder = folder
            break

    if matched_folder:
        input_directory = os.path.join(base_directory, matched_folder)
        return input_directory
    else:
        logging.error(f"Station folder for '{station_name}' not found.")
        print(f"Station folder for '{station_name}' not found.")
        return None

# Start of the script execution logic (to be run in the main section of your script)
if __name__ == "__main__":
    base_directory = 'C:/Users/ctebe/OneDrive/Desktop/SG'
    station_name = input("Enter the stream gauge name (e.g., 1410_Fagalii): ")
    start_date = input("Enter the start date (YYYY-MM-DD): ")
    end_date = input("Enter the end date (YYYY-MM-DD): ")

    input_directory = normalize_station_name(station_name, base_directory)
    if input_directory:
        output_file = os.path.join(input_directory, f'{station_name}_combined_output_single_sheet.xlsx')
        process_multiple_files_to_single_sheet(input_directory, output_file, start_date, end_date)

