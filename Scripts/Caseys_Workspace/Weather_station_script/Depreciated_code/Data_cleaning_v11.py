import pandas as pd
import os
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    filename='data_processing.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def clean_weather_data(file_path):
    try:
        print(f"Processing file: {file_path}")
        df = pd.read_excel(file_path, skiprows=3)
        print(f"File shape: {df.shape}")
        new_header_mapping = {
            'Unnamed: 0': 'Date/Time',
            'Avg': 'Swin_Avg (W/m²)',
            'Avg.1': 'Thermocouple C',
            'Smp': 'RH_Avg Percent',
            'Avg.2': 'VP_Avg (kPa)',
            'Avg.3': 'VPsat_Avg (kPa)',
            'Avg.4': 'VPD_Avg (kPa)',
            'Smp.1': 'WS_Avg (m/s)',
            'Smp.2': 'WSrs_Avg (m/s)',
            'Smp.3': 'WDuv_Avg (degrees)',
            'Smp.4': 'WDrs_Avg (degrees)',
            'Smp.5': 'WD_StdY (degrees)',
            'Smp.6': 'WD_StdCS (degrees)',
            'Tot': 'RF_Tot (mm)'
        }

        df_cleaned = df.rename(columns=new_header_mapping)
        df_cleaned = df_cleaned.drop(columns=['Unnamed: 1'], errors='ignore')

        print(f"Finished processing: {file_path}")
        return df_cleaned

    except Exception as e:
        logging.error(f"Failed to process file {file_path}: {e}")
        print(f"Error processing {file_path}: {e}")
        return None

def extract_date_from_filename(filename):
    """Attempt to extract a date from the filename assuming a date format like YYYY.MM.DD."""
    try:
        date_str = filename.split('_')[0]  # Example: '2021.12.27' from '2021.12.27_file.xlsx'
        return datetime.strptime(date_str, "%Y.%m.%d")
    except ValueError:
        return None

def process_multiple_files_to_single_sheet(input_dir, output_file, start_date, end_date):
    print(f"Processing files in directory: {input_dir}")
    combined_df = pd.DataFrame()  # Initialize an empty DataFrame

    # Define directories to be excluded
    excluded_dirs = ['Master data sheet']

    # Use os.walk to traverse through all directories and subdirectories
    for root, dirs, files in os.walk(input_dir):
        # Skip the excluded directories
        dirs[:] = [d for d in dirs if d not in excluded_dirs]

        for filename in files:
            # Skip temporary Excel files that start with '~$'
            if filename.startswith('~$'):
                print(f"Skipping temporary file: {filename}")
                continue

            file_date = extract_date_from_filename(filename)
            if file_date is None or not (start_date <= file_date <= end_date):
                continue  # Skip the file if it's outside the date range

            if filename.endswith(".xlsx") or filename.endswith(".xls"):
                file_path = os.path.join(root, filename)
                df_cleaned = clean_weather_data(file_path)

                if df_cleaned is not None and not df_cleaned.empty:
                    combined_df = pd.concat([combined_df, df_cleaned], ignore_index=True)
                    logging.info(f"Successfully processed: {file_path}")

    # Check the contents of the combined DataFrame before saving
    if combined_df.empty:
        print("No data was processed.")
    else:
        # Print some basic info to ensure the data is okay
        print("Combined DataFrame shape: ", combined_df.shape)
        print("First few rows of the data: ")
        print(combined_df.head())  # Print the first few rows to check the data

        # Write the DataFrame to Excel with the openpyxl engine
        combined_df.to_excel(output_file, index=False, engine='openpyxl')
        logging.info(f"All files processed and saved into {output_file}")
        print(f"All files processed and saved into {output_file}")

# Example usage: get the station name and date range from user input
station_name = input("Enter the weather station name (e.g., 1311_Poloa): ")
start_date_str = input("Enter the start date (YYYY-MM-DD): ")
end_date_str = input("Enter the end date (YYYY-MM-DD): ")

# Parse the dates
start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

input_directory = fr'C:\Users\ctebe\OneDrive\Desktop\GitHub Repositories\Hydro_Monitoring_Network_ASPA-UH\Final_Historical_Data\Weather_Stations\{station_name}'
output_file = fr'C:\Users\ctebe\OneDrive\Desktop\GitHub Repositories\Hydro_Monitoring_Network_ASPA-UH\Final_Historical_Data\{station_name}_combined_output_single_sheet.xlsx'

process_multiple_files_to_single_sheet(input_directory, output_file, start_date, end_date)

