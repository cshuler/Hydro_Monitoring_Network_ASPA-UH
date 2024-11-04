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

# Header variations from the text file mapped to standardized headers
header_mapping = {
    # Date and Time variations
    'Date': 'Date/Time',
    'Time': 'Date/Time',
    'Date Time, GMT-11:00': 'Date/Time',
    
    # Pressure and Temperature variations
    'Abs Pres (psi) c:1 2': 'WTlvl_Avg',
    'Abs Pres, psi': 'WTlvl_Avg',
    'Pressure': 'WTlvl_Avg',
    'Temp (°F) c:2': 'Twt_F_Avg',
    'Temp, °F': 'Twt_F_Avg',
    'Temp F': 'Twt_F_Avg',
    
    # Battery Voltage and Panel Temperature variations
    'BattVolt_Avg': 'BattVolt_Avg',
    'BattVolt_Min': 'BattVolt_Min',
    'Tpanel_Avg': 'Tpanel_Avg',
    'TCair_Avg': 'TCair_Avg',
    
    # Relative Humidity and Rainfall
    'RHenc': 'RHenc',
    'RF_Tot (mm)': 'RF_Tot (mm)',
    
    # Other variations (to ignore or handle separately)
    'Coupler Detached': None,
    'Coupler Attached': None,
    'Host Connected': None,
    'Stopped': None,
    'End Of File': None,
    'Plot Title': None
}

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

# Date extraction from filename using regex (from v10)
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

# Folder matching logic from v27 (more effective)
def find_matching_station_folder(base_directory, station_name):
    normalized_input = re.sub(r'[\.\s_]', '', station_name.lower())
    
    for folder in os.listdir(base_directory):
        normalized_folder = re.sub(r'[\.\s_]', '', folder.lower())
        if normalized_input in normalized_folder:
            return os.path.join(base_directory, folder)
    
    return None

# Main script
if __name__ == "__main__":    
    station_name = input("Enter the stream gauge name (e.g., 1410_Fagalii): ")
    start_date_str = input("Enter the start date (YYYY-MM-DD): ")
    end_date_str = input("Enter the end date (YYYY-MM-DD): ")

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    base_directory = 'C:\\Users\\ctebe\\OneDrive\\Desktop\\SG'

    # Use the folder matching logic from v27
    input_directory = find_matching_station_folder(base_directory, station_name)
    
    if input_directory:
        output_file = os.path.join(input_directory, f'{station_name}_combined_output_single_sheet.xlsx')  
        process_multiple_files_to_single_sheet(input_directory, output_file, start_date, end_date)
