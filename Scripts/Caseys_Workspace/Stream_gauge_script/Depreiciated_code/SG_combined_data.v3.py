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

# New Header Structure based on your example
NEW_HEADER = [
    'Date/ Time', 'Abs Pres (psi)', 'Temp°', 'Atmospheric Abs Pres (psi)', 'Notes'
]

# Adjust the column mapping as needed
variable_mapping = {
    'Date/ Time': ['Date/ Time', 'Datetime'],
    'Abs Pres (psi)': ['Abs Pres (psi)', 'Pressure'],
    'Temp°': ['Temp°', 'Temperature'],
    'Atmospheric Abs Pres (psi)': ['Atmospheric Abs Pres (psi)', 'Atmospheric Pressure'],
    'Notes': ['Notes']
}

# Function to clean and map the data
def clean_and_map_data(df):
    df_cleaned = pd.DataFrame()
    for new_col in NEW_HEADER:
        mapped_columns = variable_mapping.get(new_col, [])
        found_col = None
        # Find the first matching column
        for col in mapped_columns:
            if col in df.columns:
                found_col = col
                break
        
        # Add the column to the cleaned DataFrame
        if found_col:
            df_cleaned[new_col] = df[found_col]
        else:
            # If the column is missing, add a column filled with NaN
            df_cleaned[new_col] = pd.NA
    return df_cleaned

# Improved function to extract date from filename using regular expressions
def extract_date_from_filename(file_name):
    date_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', file_name)
    if date_match:
        month, day, year = date_match.groups()
        extracted_date = pd.to_datetime(f"{year}-{month}-{day}", format="%Y-%m-%d")
        logging.info(f"Successfully extracted date: {extracted_date}")
        return extracted_date
    else:
        logging.error(f"Failed to extract date from {file_name}")
        return None

# Function to clean and standardize the dataset
def clean_dataset(file_path):
    try:
        if '~$' in file_path:
            logging.info(f"Skipping temporary file: {file_path}")
            return None

        logging.info(f"Processing file: {file_path}")
        df = pd.read_excel(file_path)

        # Clean and map the data
        df_cleaned = clean_and_map_data(df)
        return df_cleaned

    except Exception as e:
        logging.error(f"Failed to process file {file_path}: {e}")
        return None

# Function to process files for a selected station and save to Excel
def process_station_files(input_directory, output_file, station_name, start_date, end_date):
    all_data = []
    station_folder = os.path.join(input_directory, station_name)
    
    if not os.path.exists(station_folder):
        print(f"Station folder '{station_name}' not found.")
        return

    for root, dirs, files in os.walk(station_folder):
        for file_name in files:
            if file_name.endswith('.xlsx'):
                file_path = os.path.join(root, file_name)
                extracted_date = extract_date_from_filename(file_name)
                if extracted_date and start_date <= extracted_date <= end_date:
                    df_cleaned = clean_dataset(file_path)
                    if df_cleaned is not None:
                        all_data.append(df_cleaned)

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df.to_excel(output_file, index=False, engine='openpyxl')
        print(f"Data for station '{station_name}' saved to {output_file}")

# Main function
def main():
    input_directory = r'C:\Users\ctebe\OneDrive\Desktop\SG'  # Main folder containing station subfolders
    station_name = input("Enter the weather stream gauge name (e.g., 1410_Fagalii): ")  # Choose specific station folder
    output_file = f'{station_name}_combined_output.xlsx'  # Output file name based on station
    
    start_date_str = input("Enter the start date (YYYY-MM-DD): ")
    end_date_str = input("Enter the end date (YYYY-MM-DD): ")

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    process_station_files(input_directory, output_file, station_name, start_date, end_date)

if __name__ == "__main__":
    main()
