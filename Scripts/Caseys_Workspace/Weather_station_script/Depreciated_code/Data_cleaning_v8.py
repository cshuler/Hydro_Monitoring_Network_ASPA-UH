import pandas as pd
import os
import logging

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

def process_multiple_files_to_single_sheet(input_dir, output_file):
    print(f"Processing files in directory: {input_dir}")
    combined_df = pd.DataFrame()  # Initialize an empty DataFrame

    # Define directories to be excluded
    excluded_dirs = ['Master data sheet']

    # Use os.walk to traverse through all directories and subdirectories
    for root, dirs, files in os.walk(input_dir):
        # Skip the excluded directories
        dirs[:] = [d for d in dirs if d not in excluded_dirs]

        for filename in files:
            if filename.endswith(".xlsx") or filename.endswith(".xls"):
                file_path = os.path.join(root, filename)
                df_cleaned = clean_weather_data(file_path)

                if df_cleaned is not None and not df_cleaned.empty:
                    combined_df = pd.concat([combined_df, df_cleaned], ignore_index=True)
                    logging.info(f"Successfully processed: {file_path}")

    if combined_df.empty:
        print("No data was processed.")
    else:
        combined_df.to_excel(output_file, index=False)
        logging.info(f"All files processed and saved into {output_file}")
        print(f"All files processed and saved into {output_file}")

# Example usage: get the station name from user input
station_name = input("Enter the weather station name (e.g., 1311_Poloa): ")

input_directory = fr'C:\Users\ctebe\OneDrive\Desktop\GitHub Repositories\Hydro_Monitoring_Network_ASPA-UH\Final_Historical_Data\Weather_Stations\{station_name}'
output_file = fr'C:\Users\ctebe\OneDrive\Desktop\GitHub Repositories\Hydro_Monitoring_Network_ASPA-UH\Final_Historical_Data\{station_name}_combined_output_single_sheet.xlsx'

process_multiple_files_to_single_sheet(input_directory, output_file)
