import pandas as pd
import os
import numpy as np
from datetime import datetime
import re

# Define thresholds or conditions for "bad" data.
bad_data_conditions = {
    'Swin_Avg (W/m²)': lambda x: x < 0 or pd.isna(x),
    'Thermocouple C': lambda x: pd.isna(x),
    'RH_Avg Percent': lambda x: x < 10 or x > 100,
    # Add other columns and conditions as needed
}

# Function to clean the PT data based on "Bad data" criteria
def clean_pt_data(pt_data, bad_data_conditions):
    bad_data_records = []

    for i in range(len(pt_data)):
        row = pt_data.iloc[i]
        bad_fields = [col for col, cond in bad_data_conditions.items() if col in pt_data.columns and cond(row[col])]

        if bad_fields:
            # Record bad data details
            bad_data_records.append({
                'Bad data Start': row['Date/Time'],
                'Bad data End': row['Date/Time'],  # Assuming bad data detected on the same row
                'Data affected': ', '.join(bad_fields),
                'Notes': 'Bad data detected'  # Optional customization
            })
            # Optionally mark the bad data in PT data (e.g., replace with NaN or flag)
            pt_data.loc[i, bad_fields] = np.nan  # Replace bad values with NaN

    return pt_data, pd.DataFrame(bad_data_records)

# Script execution
if __name__ == "__main__":
    # Load the data file
    input_file = 'C:/Users/ctebe/OneDrive/Desktop/Wx/1.3.1.2 Aasu/Data/1.3.1.2 Aasu_Wx_1.27.2024.xlsx'
    output_file = input_file  # Save to the same file

    # Load the sheets
    excel_file = pd.ExcelFile(input_file)
    pt_data = excel_file.parse('PT data')
    bad_data_existing = excel_file.parse('Bad data') if 'Bad data' in excel_file.sheet_names else pd.DataFrame()

    # Clean PT data and generate a new "Bad data" report
    cleaned_pt_data, bad_data_new = clean_pt_data(pt_data, bad_data_conditions)

    # Combine existing bad data records with new ones
    bad_data_combined = pd.concat([bad_data_existing, bad_data_new], ignore_index=True)

    # Save the updated data to the Excel file
    with pd.ExcelWriter(output_file, mode='a', engine='openpyxl') as writer:
        cleaned_pt_data.to_excel(writer, sheet_name='PT data', index=False)
        bad_data_combined.to_excel(writer, sheet_name='Bad data', index=False)
    
    print(f"Cleaned PT data and updated Bad data saved to {output_file}.")
