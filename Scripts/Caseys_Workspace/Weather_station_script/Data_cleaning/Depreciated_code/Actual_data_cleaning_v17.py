import pandas as pd
import os
import numpy as np
from datetime import datetime
import re

# Define the standard headers for the PT data and Bad data sheets
PT_DATA_HEADERS = [
    'Date/Time', 'SWin_Avg', 'Tair_Avg', 'RH_Avg', 'VP_Avg', 'VPsat_Avg', 
    'VPD_Avg', 'WS_Avg', 'WSrs_Avg', 'WDuv_Avg', 'WDrs_Avg', 'WD_StdY', 
    'WD_StdCS', 'RF_Tot'
]

BAD_DATA_HEADERS = [
    'Bad data Start', 'Bad data End', 'Data affected', 'No data from', 'To', 'Notes'
]

# Define conditions for identifying "bad data" in columns, with type checks
def create_bad_data_conditions():
    conditions = {
        'SWin_Avg': lambda x: (isinstance(x, (int, float)) and x < 0) or pd.isna(x),
        'Tair_Avg': lambda x: (isinstance(x, (int, float)) and x < 0) or pd.isna(x),
        'RH_Avg': lambda x: (isinstance(x, (int, float)) and (x < 10 or x > 100)) or pd.isna(x),
        'WS_Avg': lambda x: (isinstance(x, (int, float)) and x < 0) or pd.isna(x),
        'WSrs_Avg': lambda x: (isinstance(x, (int, float)) and x < 0) or pd.isna(x),
        'WDuv_Avg': lambda x: (isinstance(x, (int, float)) and (x < 0 or x > 360)) or pd.isna(x),
        'WD_StdY': lambda x: (isinstance(x, (int, float)) and (x < 0 or x > 360)) or pd.isna(x),
        'RF_Tot': lambda x: False,  # No restrictions on RF_Tot
    }
    return conditions

# Function to identify "bad data" in "PT data" without altering the sheet
def identify_bad_data(pt_data):
    bad_data_conditions = create_bad_data_conditions()
    bad_data_records = []

    for i in range(len(pt_data)):
        row = pt_data.iloc[i]
        bad_fields = [col for col, cond in bad_data_conditions.items() if col in pt_data.columns and cond(row[col])]

        if bad_fields:
            bad_data_records.append({
                'Bad data Start': row['Date/Time'],
                'Bad data End': row['Date/Time'],
                'Data affected': ', '.join(bad_fields),
                'No data from': '',
                'To': '',
                'Notes': 'Bad data detected'
            })

    bad_data_df = pd.DataFrame(bad_data_records, columns=BAD_DATA_HEADERS)
    print(f"Identified {len(bad_data_records)} bad data records.")
    return bad_data_df

# Main function to process files based on date range and log "bad data"
def process_and_filter_bad_data(input_directory, start_date, end_date, station_name, output_file_base):
    all_bad_data_records = []
    combined_pt_data = []
    matched_files = 0

    for root, dirs, files in os.walk(input_directory):
        for file_name in files:
            # Skip temporary or hidden files
            if file_name.startswith('~$'):
                print(f"Skipping temporary file: {file_name}")
                continue
            
            file_path = os.path.join(root, file_name)

            # Adjust date matching pattern to account for formats like "1.27.2024"
            date_pattern = rf"{start_date.month}\.{start_date.day}\.{start_date.year}"
            end_date_pattern = rf"{end_date.month}\.{end_date.day}\.{end_date.year}"
            date_match = re.search(date_pattern, file_name) or re.search(end_date_pattern, file_name)
            
            if date_match:
                matched_files += 1
                print(f"Processing file: {file_name} - Path: {file_path}")
                xl = pd.ExcelFile(file_path)

                # Check for relevant sheets
                if 'PT data' in xl.sheet_names:
                    pt_data = xl.parse('PT data')
                    pt_data.columns = PT_DATA_HEADERS  # Enforce the correct headers
                    print(f"'PT data' sheet found with {len(pt_data)} rows.")
                    
                    if not pt_data.empty:
                        bad_data_new = identify_bad_data(pt_data)
                        combined_pt_data.append(pt_data)
                        all_bad_data_records.append(bad_data_new)
                    else:
                        print("The 'PT data' sheet is empty.")
                else:
                    print(f"No 'PT data' sheet found in file: {file_name}")
            else:
                print(f"File '{file_name}' does not match date criteria.")

    if matched_files == 0:
        print("No files matched the specified date range.")

    pt_data_combined = pd.concat(combined_pt_data, ignore_index=True) if combined_pt_data else pd.DataFrame(columns=PT_DATA_HEADERS)
    bad_data_combined = pd.concat(all_bad_data_records, ignore_index=True) if all_bad_data_records else pd.DataFrame(columns=BAD_DATA_HEADERS)

    os.makedirs(os.path.dirname(output_file_base), exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{output_file_base}_{timestamp}.xlsx"

    date_station_title = f"{start_date.strftime('%m.%d.%Y')} {station_name}"

    with pd.ExcelWriter(output_file, mode='w', engine='openpyxl') as writer:
        pt_data_combined.to_excel(writer, sheet_name='PT data', index=False)
        bad_data_combined.to_excel(writer, sheet_name='Bad data', index=False, startrow=1)
        worksheet = writer.sheets['Bad data']
        worksheet.cell(row=1, column=1, value=date_station_title)

    print(f"Original PT data and identified Bad data saved to {output_file}.")
    print(f"PT data rows: {len(pt_data_combined)}, Bad data rows: {len(bad_data_combined)}")

# Script execution
if __name__ == "__main__":
    station_name = input("Enter the weather station name (e.g., 1311_Poloa): ")
    start_date_str = input("Enter the start date (YYYY-MM-DD): ")
    end_date_str = input("Enter the end date (YYYY-MM-DD): ")

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    input_directory = f'C:/Users/ctebe/OneDrive/Desktop/Wx/1.3.1.2 Aasu/Data'
    output_file_base = (f'C:/Users/ctebe/OneDrive/Desktop/Wx/1.3.1.2 Aasu/Data/'
                        f'{station_name}_filtered_bad_data_{start_date.strftime("%Y-%m-%d")}_to_{end_date.strftime("%Y-%m-%d")}')

    process_and_filter_bad_data(input_directory, start_date, end_date, station_name, output_file_base)
