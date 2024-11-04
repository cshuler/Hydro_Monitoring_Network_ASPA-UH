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

# Adjusted variable mapping for flexible column names
column_mapping_dict = {
    'Date/Time': ['Date/Time', 'Datetime', 'Timestamp'],
    'WTlvl_Avg': ['Abs Pres (psi)', 'Pressure', 'Abs Pressure'],
    'Twt_F_Avg': ['Temp°', 'Temp', 'Temperature', 'Temp F'],
    'BattVolt_Avg': ['Volts', 'Battery Voltage', 'BattVolt_Avg'],
    'BattVolt_Min': ['Min Volts', 'BattVolt_Min'],
    'Tpanel_Avg': ['Panel Temp', 'Tpanel_Avg'],
    'TCair_Avg': ['Air Temp', 'TCair_Avg'],
    'RHenc': ['RHenc', 'Relative Humidity'],
    'RF_Tot (mm)': ['Rainfall', 'RF_Tot (mm)', 'Rainfall (mm)']
}

# Function to map columns to the standardized header
def map_columns(df):
    print(f"Mapping columns for file with headers: {df.columns.tolist()}")
    new_columns = {}
    for standard_col, variations in column_mapping_dict.items():
        for col in variations:
            if col in df.columns:
                new_columns[col] = standard_col
                break
    df = df.rename(columns=new_columns)

    # Add missing columns
    for std_col in standardized_header:
        if std_col not in df.columns:
            df[std_col] = None  # Add empty columns for missing data
    print(f"Final mapped columns: {df.columns.tolist()}")
    return df[standardized_header]  # Return reordered columns

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

# Function to clean and process XLSX files with "PT data" sheet handling
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
        return clean_csv_data(file_path)
    elif file_path.endswith('.xlsx'):
        return clean_xlsx_data(file_path)
    else:
        logging.warning(f"Unsupported file format: {file_path}")
        print(f"Unsupported file format: {file_path}")
        return None

# Function to combine and clean data for the specified station
def process_multiple_files_to_single_sheet(input_directory, output_file, start_date, end_date):
    all_data = []
    logging.info("Starting the script.")
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
        logging.info(f"Saving data to {output_file}")
        combined_df.to_excel(output_file, index=False, engine='openpyxl')
        logging.info(f"Data successfully saved to {output_file}")
        print(f"Data saved to: {output_file}")
        print("Final combined data preview:")
        print(combined_df.head())
    else:
        logging.info("No valid data found.")
        print("No valid data found.")

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
            print(f"Failed to extract date from filename: {file_name}")
            return None
    except Exception as e:
        logging.error(f"Error extracting date from {file_name}: {e}")
        print(f"Error extracting date from filename: {file_name}")
        return None

# Normalize station name for matching
def normalize_station_name(station_name, base_directory):
    matched_folder = None
    
    # Modify station name to match "1.4.1.1" format
    if station_name[:4].isdigit():
        station_number = f"{station_name[:1]}.{station_name[1:2]}.{station_name[2:3]}.{station_name[3:4]}"
        station_name_clean = station_number + station_name[4:].lower().replace("_", "").replace("-", "").replace(".", "").strip()
    else:
        station_name_clean = station_name.lower().replace("_", "").replace("-", "").replace(".", "").strip()

    # Debugging: Print all folders in the base directory
    print("Available folders in base directory:")
    for folder in os.listdir(base_directory):
        print(f"Folder: {folder}")
    
    for folder in os.listdir(base_directory):
        # Clean the folder name for matching
        folder_clean = folder.lower().replace("_", "").replace("-", "").replace(".", "").strip()
        
        # Check if the station_name_clean matches the folder name
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

# Main script
if __name__ == "__main__":
    station_name = input("Enter the stream gauge name (e.g., 1410_Fagalii): ")
    start_date_str = input("Enter the start date (YYYY-MM-DD): ")
    end_date_str = input("Enter the end date (YYYY-MM-DD): ")

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    base_directory = 'C:\\Users\\ctebe\\OneDrive\\Desktop\\SG'

    # Use the normalize function to get the directory
    input_directory = normalize_station_name(station_name, base_directory)
    
    if input_directory:
        output_file = os.path.join(input_directory, f'{station_name}_combined_output_single_sheet.xlsx')  # Save in the same folder
        process_multiple_files_to_single_sheet(input_directory, output_file, start_date, end_date)
