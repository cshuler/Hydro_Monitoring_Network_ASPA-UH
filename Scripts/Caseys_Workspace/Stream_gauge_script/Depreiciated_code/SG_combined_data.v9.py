import os
import pandas as pd
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

# Define the custom header for stream gauge data processing
SG_HEADER = [
    'Date/ Time', 'Abs Pres (psi)', 'Temp°', 'Atmospheric Abs Pres (psi)', 'Notes'
]

# Variable mapping for both CSV and Excel data
variable_mapping = {
    'Date/ Time': ['Date/ Time', 'Datetime', 'Timestamp'],
    'Abs Pres (psi)': ['Abs Pres (psi)', 'Pressure'],
    'Temp°': ['Temp°', 'Temperature', 'Temp'],
    'Atmospheric Abs Pres (psi)': ['Atmospheric Abs Pres (psi)', 'Atmospheric Pressure'],
    'Notes': ['Notes', 'Comments']
}

# Function to map columns to the standard header
def map_columns(df):
    new_columns = {}
    for standard_col, variations in variable_mapping.items():
        for col in variations:
            if col in df.columns:
                new_columns[col] = standard_col
                break
    return df.rename(columns=new_columns)

# Function to clean and map data according to the standard header
def clean_and_map_data(file_path, file_type):
    try:
        print(f"Processing file: {file_path}")
        if file_type == 'csv':
            df = pd.read_csv(file_path)
        elif file_type == 'xlsx':
            xl = pd.ExcelFile(file_path)
            if 'PT data' in xl.sheet_names:
                df = pd.read_excel(file_path, sheet_name='PT data')
            else:
                print(f"'PT data' sheet not found in {file_path}. Skipping file.")
                return None
        else:
            print(f"Unsupported file type: {file_type}")
            return None
        
        # Map columns to the standard header
        df_cleaned = map_columns(df).reindex(columns=SG_HEADER, fill_value=pd.NA)
        print(f"Finished processing: {file_path}")
        return df_cleaned
    except Exception as e:
        logging.error(f"Failed to process file {file_path}: {e}")
        print(f"Error processing {file_path}: {e}")
        return None

# Improved function to extract date from filename using regular expressions
def extract_date_from_filename(file_name):
    try:
        date_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', file_name)
        if date_match:
            month, day, year = date_match.groups()
            extracted_date = pd.to_datetime(f"{year}-{month}-{day}", format="%Y-%m-%d")
            logging.info(f"Successfully extracted date: {extracted_date}")
            return extracted_date
        else:
            logging.error(f"Failed to extract date from {file_name}")
            return None
    except Exception as e:
        logging.error(f"Failed to extract date from {file_name}: {e}")
        return None

# Function to process multiple files in the station directory
def process_multiple_files(station_folder, start_date, end_date, output_file):
    combined_df = pd.DataFrame()  # Initialize an empty DataFrame

    for root, dirs, files in os.walk(station_folder):
        if 'Master data sheet' in root or 'Data' not in root:
            continue

        for filename in files:
            file_path = os.path.join(root, filename)
            file_type = 'csv' if filename.endswith('.csv') else 'xlsx' if filename.endswith('.xlsx') else None
            
            if file_type:
                # Extract date from filename and filter files within the date range
                extracted_date = extract_date_from_filename(filename)
                
                if extracted_date and start_date <= extracted_date <= end_date:
                    df_cleaned = clean_and_map_data(file_path, file_type)
                    if df_cleaned is not None:
                        combined_df = pd.concat([combined_df, df_cleaned], ignore_index=True)
                        logging.info(f"Successfully processed: {file_path}")

    # Write the combined data to the output Excel file
    try:
        if not combined_df.empty:
            print(f"Saving data to {output_file}")
            combined_df.to_excel(output_file, index=False, engine='openpyxl')
            logging.info(f"Data successfully saved to {output_file}")
        else:
            print("No data to save.")
    except Exception as e:
        logging.error(f"Failed to save the file {output_file}: {e}")
        print(f"Error saving the file: {e}")

# New part: folder matching logic with normalization
def find_matching_station_folder(base_directory, station_name):
    # Normalize station name to remove special characters (dots, spaces, underscores)
    normalized_input = re.sub(r'[\.\s_]', '', station_name.lower())
    
    for folder in os.listdir(base_directory):
        # Normalize folder name in the same way
        normalized_folder = re.sub(r'[\.\s_]', '', folder.lower())
        
        # If the normalized names match, return the folder
        if normalized_input in normalized_folder:
            return os.path.join(base_directory, folder)
    
    return None

# Main function to handle user inputs and start the process
def main():
    station_name = input("Enter the stream gauge name (e.g., 1410_Fagalii): ")
    start_date = datetime.strptime(input("Enter the start date (YYYY-MM-DD): "), "%Y-%m-%d")
    end_date = datetime.strptime(input("Enter the end date (YYYY-MM-DD): "), "%Y-%m-%d")
    
    base_directory = 'C:\\Users\\ctebe\\OneDrive\\Desktop\\SG'
    station_folder = find_matching_station_folder(base_directory, station_name)
    
    if not station_folder:
        print(f"Station folder for '{station_name}' not found.")
        return
    
    output_file = os.path.join(station_folder, f"{station_name}_combined_output_single_sheet.xlsx")
    process_multiple_files(station_folder, start_date, end_date, output_file)

if __name__ == "__main__":
    main()
