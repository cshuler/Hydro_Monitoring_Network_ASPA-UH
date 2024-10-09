import pandas as pd
import os
import re
from datetime import datetime

# Standardized header you want to use
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
def clean_csv_data(file_path, standardized_header):
    try:
        print(f"Processing CSV file: {file_path}")
        df = pd.read_csv(file_path)
        df = map_columns(df, standardized_header)
        return df
    except Exception as e:
        print(f"Failed to process CSV {file_path}: {e}")
        return None

# Function to clean and process XLSX files
def clean_xlsx_data(file_path, standardized_header):
    try:
        print(f"Processing XLSX file: {file_path}")
        df = pd.read_excel(file_path)
        df = map_columns(df, standardized_header)
        return df
    except Exception as e:
        print(f"Failed to process XLSX {file_path}: {e}")
        return None

# Main processing function to handle multiple files and create a combined DataFrame
def process_multiple_files_to_single_sheet(input_directory, output_file, start_date, end_date):
    all_data = []
    print("Starting the script...")

    # Walk through directory and process files
    for root, dirs, files in os.walk(input_directory):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            if file_name.endswith('.csv') or file_name.endswith('.xlsx'):
                # Extract date from filename and filter based on date range
                extracted_date = extract_date_from_filename(file_name)
                if extracted_date and start_date <= extracted_date <= end_date:
                    if file_name.endswith('.csv'):
                        df_cleaned = clean_csv_data(file_path, standardized_header)
                    elif file_name.endswith('.xlsx'):
                        df_cleaned = clean_xlsx_data(file_path, standardized_header)
                    
                    if df_cleaned is not None:
                        all_data.append(df_cleaned)

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df.columns = standardized_header  # Apply the standardized header
        print(f"Saving data to {output_file}")
        combined_df.to_excel(output_file, index=False, engine='openpyxl')
        print(f"Data saved to: {output_file}")
    else:
        print("No valid data found.")

# Function to extract date from filename
def extract_date_from_filename(file_name):
    try:
        date_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', file_name)
        if date_match:
            month, day, year = date_match.groups()
            return pd.to_datetime(f"{year}-{month}-{day}", format="%Y-%m-%d")
    except Exception as e:
        print(f"Error extracting date from {file_name}: {e}")
    return None

# Main script
if __name__ == "__main__":
    station_name = input("Enter the stream gauge name (e.g., 1410_Fagalii): ")
    start_date_str = input("Enter the start date (YYYY-MM-DD): ")
    end_date_str = input("Enter the end date (YYYY-MM-DD): ")

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    base_directory = 'C:\\Users\\ctebe\\OneDrive\\Desktop\\SG'
    matched_folder = find_station_folder(station_name, base_directory)

    if matched_folder:
        input_directory = os.path.join(base_directory, matched_folder)
        output_file = os.path.join(base_directory, f'{station_name}_combined_output_single_sheet.xlsx')
        process_multiple_files_to_single_sheet(input_directory, output_file, start_date, end_date)
    else:
        print(f"Station folder for '{station_name}' not found.")
