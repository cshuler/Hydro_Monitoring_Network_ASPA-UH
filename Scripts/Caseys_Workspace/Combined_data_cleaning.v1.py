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

# Define the standard header
STANDARD_HEADER = [
    'Date/Time', 'Swin_Avg (W/m²)', 'Thermocouple C', 'RH_Avg Percent',
    'VP_Avg (kPa)', 'VPsat_Avg (kPa)', 'VPD_Avg (kPa)', 'WS_Avg (m/s)',
    'WSrs_Avg (m/s)', 'WDuv_Avg (degrees)', 'WDrs_Avg (degrees)',
    'WD_StdY (degrees)', 'WD_StdCS (degrees)', 'RF_Tot (mm)'
]

# Function to clean and standardize weather data
def clean_weather_data(file_path):
    try:
        print(f"Processing file: {file_path}")
        # Load the sheet names for debugging
        xl = pd.ExcelFile(file_path)
        print(xl.sheet_names)  # Print all sheet names for debugging
        
        # Load the data from the appropriate sheet
        if 'PT data' in xl.sheet_names:
            df = pd.read_excel(file_path, sheet_name='PT data')
        elif 'PT data (5min)' in xl.sheet_names:
            df = pd.read_excel(file_path, sheet_name='PT data (5min)')
        else:
            print(f"No 'PT data' or 'PT data (5min)' sheet found in {file_path}")
            return None

        print(f"File shape: {df.shape}")

        # Reindex the DataFrame to match the standard header, filling missing columns with NaN
        df_cleaned = df.rename(columns=new_header_mapping())
        df_cleaned = df_cleaned.reindex(columns=STANDARD_HEADER, fill_value=pd.NA)

        print(f"Finished processing: {file_path}")
        return df_cleaned

    except Exception as e:
        logging.error(f"Failed to process file {file_path}: {e}")
        print(f"Error processing {file_path}: {e}")
        return None

# Function to define header mapping from your files
def new_header_mapping():
    return {
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

# Function to process multiple files and save them into a single Excel sheet
def process_multiple_files_to_single_sheet(input_dir, output_file):
    print(f"Processing files in directory: {input_dir}")
    combined_df = pd.DataFrame()  # Initialize an empty DataFrame

    # Exclude directories (e.g., Master data sheet)
    excluded_dirs = ['Master data sheet']

    # Walk through directories and files
    for root, dirs, files in os.walk(input_dir):
        # Skip the excluded directories
        dirs[:] = [d for d in dirs if d not in excluded_dirs]

        for filename in files:
            # Skip temporary Excel files that start with '~$'
            if filename.startswith('~$'):
                print(f"Skipping temporary file: {filename}")
                continue

            if filename.endswith(".xlsx") or filename.endswith(".xls"):
                file_path = os.path.join(root, filename)
                df_cleaned = clean_weather_data(file_path)

                if df_cleaned is not None and not df_cleaned.empty:
                    combined_df = pd.concat([combined_df, df_cleaned], ignore_index=True)
                    logging.info(f"Successfully processed: {file_path}")

    # Check if any data was processed
    if combined_df.empty:
        print("No data was processed.")
    else:
        print(f"Combined DataFrame shape: {combined_df.shape}")
        print("First few rows of the data:")
        print(combined_df.head())  # Print the first few rows of combined data for verification

        # Write the combined DataFrame to Excel
        combined_df.to_excel(output_file, index=False, engine='openpyxl')
        logging.info(f"All files processed and saved into {output_file}")
        print(f"All files processed and saved into {output_file}")

# Main function to select the weather station and output file
def main():
    station_name = input("Enter the weather station name (e.g., 1311_Poloa): ")
    input_directory = fr'C:\Users\ctebe\OneDrive\Desktop\GitHub Repositories\Hydro_Monitoring_Network_ASPA-UH\Final_Historical_Data\Weather_Stations\{station_name}'
    output_file = fr'C:\Users\ctebe\OneDrive\Desktop\GitHub Repositories\Hydro_Monitoring_Network_ASPA-UH\Final_Historical_Data\{station_name}_combined_output_single_sheet.xlsx'

    print(f"Processing files in directory: {input_directory}")
    process_multiple_files_to_single_sheet(input_directory, output_file)

# Execute the main function when the script is run
if __name__ == "__main__":
    main()
