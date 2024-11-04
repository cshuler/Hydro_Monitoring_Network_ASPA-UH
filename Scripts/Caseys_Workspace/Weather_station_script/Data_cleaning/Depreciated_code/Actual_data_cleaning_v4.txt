import pandas as pd
import os
import numpy as np
from datetime import datetime
import re
import logging

# Define thresholds or conditions for "bad" data.
bad_data_conditions = {
    'Swin_Avg (W/m²)': lambda x: x < 0 or pd.isna(x),
    'Thermocouple C': lambda x: pd.isna(x),
    'RH_Avg Percent': lambda x: x < 10 or x > 100,
    # Add other columns and conditions as needed
}

# Function to extract date from filename
def extract_date_from_filename(file_name):
    date_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', file_name)
    if date_match:
        month, day, year = date_match.groups()
        return pd.to_datetime(f"{year}-{month}-{day}", format="%Y-%m-%d")
    return None

# Main function to load, clean, and filter data based on site and date range
def process_and_filter_bad_data(input_directory, start_date, end_date, output_file):
    bad_data_records = []

    for root, dirs, files in os.walk(input_directory):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            extracted_date = extract_date_from_filename(file_name)

            # Check if the file is within the desired date range
            if extracted_date and start_date <= extracted_date <= end_date:
                xl = pd.ExcelFile(file_path)

                # Check for relevant sheets
                if 'PT data' in xl.sheet_names:
                    pt_data = xl.parse('PT data')
                    # Iterate through rows to identify bad data
                    for i in range(len(pt_data)):
                        row = pt_data.iloc[i]
                        bad_fields = [col for col, cond in bad_data_conditions.items() if cond(row.get(col, None))]

                        # Log bad data records
                        if bad_fields:
                            if not bad_data_records or bad_data_records[-1]['End Time'] != row['Date/Time']:
                                bad_data_records.append({
                                    'Start Time': row['Date/Time'],
                                    'End Time': row['Date/Time'],
                                    'Data affected': ', '.join(bad_fields),
                                    'Notes': 'Bad data detected'
                                })
                            else:
                                bad_data_records[-1]['End Time'] = row['Date/Time']

    # Convert the bad data records to a DataFrame
    bad_data_df = pd.DataFrame(bad_data_records, columns=['Start Time', 'End Time', 'Data affected', 'Notes'])

    # Ensure the output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Save to Excel
    with pd.ExcelWriter(output_file, mode='w') as writer:
        bad_data_df.to_excel(writer, sheet_name='Processed Bad Data', index=False)

# Script execution
if __name__ == "__main__":
    station_name = input("Enter the weather station name (e.g., 1311_Poloa): ")
    start_date_str = input("Enter the start date (YYYY-MM-DD): ")
    end_date_str = input("Enter the end date (YYYY-MM-DD): ")

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    input_directory = f'C:/Users/ctebe/OneDrive/Desktop/Wx/{station_name}'
    
    # Format the dates to be part of the output filename
    output_file = (f'C:/Users/ctebe/OneDrive/Desktop/Wx/1.3.1.2 Aasu/Data/'
                   f'{station_name}_filtered_bad_data_{start_date.strftime("%Y-%m-%d")}_to_{end_date.strftime("%Y-%m-%d")}.xlsx')

    process_and_filter_bad_data(input_directory, start_date, end_date, output_file)
    print(f"Filtered bad data has been saved to {output_file}.")
