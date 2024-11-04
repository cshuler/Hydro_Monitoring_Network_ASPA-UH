import pandas as pd
import numpy as np
from datetime import datetime
import os
import re

# Define conditions for identifying "bad data" in columns
def create_bad_data_conditions():
    conditions = {
        'Swin_Avg (W/m²)': lambda x: x < 0 or pd.isna(x),
        'Thermocouple C': lambda x: x < 0 or pd.isna(x),
        'RH_Avg Percent': lambda x: x < 10 or x > 100 or pd.isna(x),
        'WS_Avg (m/s)': lambda x: x < 0 or pd.isna(x),
        'WSrs_Avg (m/s)': lambda x: x < 0 or pd.isna(x),
        'WDuv_Avg (degrees)': lambda x: x < 0 or x > 360 or pd.isna(x),
        'WD_StdY (degrees)': lambda x: x < 0 or x > 360 or pd.isna(x),
        'Tsoil_1 C': lambda x: x < 0 or pd.isna(x),
        'Battery Volts': lambda x: x < 11 or x < 0 or pd.isna(x),
        # Special handling for VPD_Avg and RF_Tot
        'VPD_Avg (kPa)': lambda row: row['VPD_Avg (kPa)'] < 0 if row['RH_Avg Percent'] != 100 else False,
        'RF_Tot (mm)': lambda x: False,  # No restrictions on RF_Tot
    }
    return conditions

# Function to find the file based on station name and date pattern
def find_matching_file(directory, station_name, date_str):
    date_pattern = datetime.strptime(date_str, "%Y-%m-%d").strftime("%m.%d.%Y")
    regex_pattern = re.compile(rf".*{station_name}.*{date_pattern}.*\.xlsx", re.IGNORECASE)
    
    for file_name in os.listdir(directory):
        if regex_pattern.match(file_name):
            return os.path.join(directory, file_name)
    return None

# Main function to process the specified Excel file and log "bad data"
def process_file(input_file, output_file):
    # Load the relevant sheets from the raw data file
    xl = pd.ExcelFile(input_file)

    # Load "PT data" without modification
    pt_data = xl.parse('PT data')

    # Identify bad data without altering PT data
    bad_data_df = identify_bad_data(pt_data)

    # Write the original PT data and identified Bad data to the output file
    with pd.ExcelWriter(output_file, mode='w', engine='openpyxl') as writer:
        pt_data.to_excel(writer, sheet_name='PT data', index=False)  # Original PT data
        bad_data_df.to_excel(writer, sheet_name='Bad data', index=False)  # Identified bad data

    print(f"Original PT data and identified Bad data saved to {output_file}.")

# Script execution
if __name__ == "__main__":
    # Prompt for station name and date
    station_name = input("Enter the weather station name (e.g., 1312_Aasu): ")
    date_str = input("Enter the date (YYYY-MM-DD): ")

    # Directory path
    directory = f'C:/Users/ctebe/OneDrive/Desktop/Wx/1.3.1.2 Aasu/Data'
    
    # Find the file based on the provided pattern
    input_file = find_matching_file(directory, station_name, date_str)
    if input_file:
        # Construct output file name
        output_file = os.path.join(directory, f"{station_name}_Wx_{datetime.strptime(date_str, '%Y-%m-%d').strftime('%m.%d.%Y')}_cleaned.xlsx")
        
        # Process the file
        process_file(input_file, output_file)
    else:
        print(f"No file found for station '{station_name}' on date '{date_str}'. Please verify the file name and format.")
