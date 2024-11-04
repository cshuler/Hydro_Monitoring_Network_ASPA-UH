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

# Define the standard header (from the provided image)
STANDARD_HEADER = [
    'Date/Time', 'Battery Volts', 'Battery Volts, MIN', 'Swin_Avg (W/m²)', 'Irradiance (Tot)', 
    'Air Temp F', 'Tair_ Avg C', 'RH_Avg Percent', 'VP_Avg (kPa)', 'VPsat_Avg (kPa)', 'VPD_Avg (kPa)', 
    'Wind Speed (MPH)', 'WS_Avg (m/s)', 'WSrs_Avg (m/s)', 'WDuv_Avg (degrees)', 'WDrs_Avg (degrees)', 
    'WD_StdY (degrees)', 'WD_StdCS (degrees)', 'SM_1_Avg (m3/m3)', 'Tsoil_1 C', 'Precipitation', 
    'RF_Tot (mm)', 'Thermocouple'
]

# Define a mapping from variable variations to the standardized header format
variable_mapping = {
    'Date/Time': ['Date/Time'],
    'Swin_Avg (W/m²)': ['Swin_Avg (W/m²)', 'Irradiance (AVG)', 'Irradiance (Tot)', 'Swin_Avg'],
    'Thermocouple C': ['Thermocouple', 'Temp C', 'Tair_ Avg C', 'Tair_Avg C'],
    'RH_Avg Percent': ['RH_Avg Percent', 'RH Percent', 'RH'],
    'VP_Avg (kPa)': ['VP_Avg (kPa)', 'VP_Avg'],
    'VPsat_Avg (kPa)': ['VPsat_Avg (kPa)', 'VPsat_Avg'],
    'VPD_Avg (kPa)': ['VPD_Avg (kPa)', 'VPD_Avg'],
    'WS_Avg (m/s)': ['WS_Avg (m/s)', 'Wind Speed (MPH)', 'Wind Speed'],
    'WSrs_Avg (m/s)': ['WSrs_Avg (m/s)', 'Wind Vector SD (Deg)', 'Wind Vector SD'],
    'WDuv_Avg (degrees)': ['WDuv_Avg (degrees)', 'Wind Direction (Deg)', 'Wind Direction'],
    'WDrs_Avg (degrees)': ['WDrs_Avg (degrees)'],
    'WD_StdY (degrees)': ['WD_StdY (degrees)', 'WD_StdY'],
    'WD_StdCS (degrees)': ['WD_StdCS (degrees)', 'WD Std Dev (Deg)', 'WD Std Dev'],
    'SM_1_Avg (m3/m3)': ['SM_1_Avg (m3/m3)', 'Soil Moisture'],
    'Tsoil_1 C': ['Tsoil_1 C', 'Tsoil C'],
    'RF_Tot (mm)': ['RF_Tot (mm)', 'Precipitation', 'Rainfall'],
}

def clean_weather_data(file_path):
    try:
        print(f"Processing file: {file_path}")
        # Load the Excel file to check available sheet names
        xl = pd.ExcelFile(file_path)
        print(xl.sheet_names)  # Print all sheet names for debugging

        # Check for the presence of specific sheets
        if 'PT data' in xl.sheet_names:
            df = pd.read_excel(file_path, sheet_name='PT data')
        elif 'PT data (5min)' in xl.sheet_names:
            df = pd.read_excel(file_path, sheet_name='PT data (5min)')
        else:
            print(f"No 'PT data' or 'PT data (5min)' sheet found in {file_path}")
            return None

        print(f"File shape: {df.shape}")

        # Create a new dictionary for column mapping based on the file
        column_mapping = {}

        for standard_col, variations in variable_mapping.items():
            for variation in variations:
                if variation in df.columns:
                    column_mapping[variation] = standard_col
                    break

        # Rename columns using the mapping
        df_cleaned = df.rename(columns=column_mapping)

        # Reindex the DataFrame to match the desired columns and leave missing columns blank
        df_cleaned = df_cleaned.reindex(columns=STANDARD_HEADER, fill_value='')

        print(f"Finished processing: {file_path}")
        return df_cleaned

    except Exception as e:
        logging.error(f"Failed to process file {file_path}: {e}")
        print(f"Error processing {file_path}: {e}")
        return None

def extract_date_from_filename(filename):
    """Attempt to extract a date from the filename assuming multiple date formats."""
    print(f"Trying to extract date from filename: {filename}")
    date_pattern = r"(\d{1,2})\.(\d{1,2})\.(\d{4})"
    
    match = re.search(date_pattern, filename)
    
    if match:
        day, month, year = match.groups()
        date_str = f"{day}.{month}.{year}"
        date_formats = ["%d.%m.%Y", "%m.%d.%Y", "%Y.%m.%d"]
        
        for date_format in date_formats:
            try:
                date = datetime.strptime(date_str, date_format)
                print(f"Extracted date: {date} using format {date_format}")
                return date
            except ValueError:
                continue
    
    print(f"Failed to extract date from filename: {filename}")
    return None

def process_multiple_files_to_single_sheet(input_dir, output_file, start_date, end_date):
    print(f"Processing files in directory: {input_dir}")
    combined_df = pd.DataFrame()

    excluded_dirs = ['Master data sheet']

    for root, dirs, files in os.walk(input_dir):
        dirs[:] = [d for d in dirs if d not in excluded_dirs]

        for filename in files:
            if filename.startswith('~$'):
                print(f"Skipping temporary file: {filename}")
                continue

            file_date = extract_date_from_filename(filename)
            if file_date is None or not (start_date <= file_date <= end_date):
                continue

            if filename.endswith(".xlsx") or filename.endswith(".xls"):
                file_path = os.path.join(root, filename)
                df_cleaned = clean_weather_data(file_path)

                if df_cleaned is not None and not df_cleaned.empty:
                    combined_df = pd.concat([combined_df, df_cleaned], ignore_index=True)
                    logging.info(f"Successfully processed: {file_path}")

    if combined_df.empty:
        print("No data was processed.")
    else:
        print("Combined DataFrame shape: ", combined_df.shape)
        print("First few rows of the data: ")
        print(combined_df.head())

        try:
            combined_df.to_excel(output_file, index=False, engine='openpyxl')
            logging.info(f"All files processed and saved into {output_file}")
            print(f"All files processed and saved into {output_file}")
        except Exception as e:
            print(f"Error saving file: {e}")
            logging.error(f"Failed to save file {output_file}: {e}")

# Main function
if __name__ == "__main__":
    station_name = input("Enter the weather station name (e.g., 1311_Poloa): ")
    start_date_str = input("Enter the start date (YYYY-MM-DD): ")
    end_date_str = input("Enter the end date (YYYY-MM-DD): ")

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    input_directory = fr'C:\Users\ctebe\OneDrive\Desktop\GitHub Repositories\Hydro_Monitoring_Network_ASPA-UH\Final_Historical_Data\Weather_Stations\{station_name}'
    output_file = fr'C:\Users\ctebe\OneDrive\Desktop\GitHub Repositories\Hydro_Monitoring_Network_ASPA-UH\Final_Historical_Data\{station_name}_combined_output_single_sheet.xlsx'

    process_multiple_files_to_single_sheet(input_directory, output_file, start_date, end_date)
