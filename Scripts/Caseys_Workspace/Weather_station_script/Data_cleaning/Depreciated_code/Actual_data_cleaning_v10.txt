import pandas as pd
import numpy as np
from datetime import datetime
import os
import re

# Define conditions for identifying "bad data" in columns
bad_data_conditions = {
    'Swin_Avg (W/m²)': lambda x: x < 0 or pd.isna(x),
    'Thermocouple C': lambda x: pd.isna(x),
    'RH_Avg Percent': lambda x: x < 10 or x > 100,
    # Add other conditions if needed
}

# Function to identify "bad data" in "PT data" without altering the sheet
def identify_bad_data(pt_data, bad_data_conditions):
    bad_data_records = []
    for i in range(len(pt_data)):
        row = pt_data.iloc[i]
        bad_fields = [col for col, cond in bad_data_conditions.items() if col in pt_data.columns and cond(row[col])]

        if bad_fields:
            # Record bad data details for logging in the "Bad data" sheet
            bad_data_records.append({
                'Bad data Start': row['Date/Time'],
                'Bad data End': row['Date/Time'],
                'Data affected': ', '.join(bad_fields),
                'Notes': 'Bad data detected'
            })

    # Create "Bad data" DataFrame with the same headers as the original
    bad_data_df = pd.DataFrame(bad_data_records, columns=['Bad data Start', 'Bad data End', 'Data affected', 'Notes'])
    return bad_data_df

# Main function to process the specified Excel file and log "bad data"
def process_file(input_file, output_file):
    # Load the relevant sheets from the raw data file
    xl = pd.ExcelFile(input_file)
    
    # Load "PT data" without modification
    pt_data = xl.parse('PT data')
    
    # Identify bad data without altering PT data
    bad_data_df = identify_bad_data(pt_data, bad_data_conditions)
    
    # Write the original PT data and identified Bad data to the output file
    with pd.ExcelWriter(output_file, mode='w', engine='openpyxl') as writer:
        pt_data.to_excel(writer, sheet_name='PT data', index=False)  # Original PT data
        bad_data_df.to_excel(writer, sheet_name='Bad data', index=False)  # Identified bad data

    print(f"Original PT data and identified Bad data saved to {output_file}.")

# Script execution
if __name__ == "__main__":
    # Prompt for station name and date
    station_name = input("Enter the weather station name (e.g., 1311_Poloa): ")
    date_str = input("Enter the date (YYYY-MM-DD): ")
    date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%m.%d.%Y")

    # Construct file paths
    input_file = f'C:/Users/ctebe/OneDrive/Desktop/Wx/{station_name}/{station_name}_Wx_{date}_exampleGPT.xlsx'
    output_file = f'C:/Users/ctebe/OneDrive/Desktop/Wx/{station_name}/{station_name}_Wx_{date}_cleaned.xlsx'
    
    # Ensure the input file exists
    if not os.path.exists(input_file):
        print(f"Input file '{input_file}' not found.")
    else:
        # Process the file
        process_file(input_file, output_file)
