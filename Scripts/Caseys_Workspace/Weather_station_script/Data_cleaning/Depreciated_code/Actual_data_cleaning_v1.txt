import pandas as pd
import numpy as np

# Load the data
file_path = '/path/to/your/file.xlsx'
excel_file = pd.ExcelFile(file_path)

# Load sheets
pt_data = excel_file.parse('PT data')

# Define thresholds or conditions for "bad" data. Adjust as needed.
# Example: Define columns where missing or specific values denote "bad" data
bad_data_conditions = {
    'Swin_Avg (W/m²)': lambda x: x < 0 or pd.isna(x),
    'Thermocouple C': lambda x: pd.isna(x),
    'RH_Avg Percent': lambda x: x < 10 or x > 100,
    # Add other columns and conditions as needed
}

# DataFrame to store bad data records
bad_data_records = []

# Iterate through rows to check for bad data
for i in range(len(pt_data)):
    bad_fields = []
    row = pt_data.iloc[i]
    
    # Check each column for bad data based on the defined conditions
    for column, condition in bad_data_conditions.items():
        if condition(row[column]):
            bad_fields.append(column)
    
    # If any bad data is found in the row, log it
    if bad_fields:
        if not bad_data_records or bad_data_records[-1]['End Time'] != row['Date/Time']:
            # Start a new bad data record
            bad_data_records.append({
                'Start Time': row['Date/Time'],
                'End Time': row['Date/Time'],
                'Data affected': ', '.join(bad_fields),
                'Notes': 'Bad data detected'  # Customize notes if needed
            })
        else:
            # Extend the end time of the last record
            bad_data_records[-1]['End Time'] = row['Date/Time']

# Convert the bad data records into a DataFrame matching the "Bad data" sheet structure
bad_data_df = pd.DataFrame(bad_data_records, columns=['Start Time', 'End Time', 'Data affected', 'Notes'])

# Save to a new Excel file or add it as a new sheet
with pd.ExcelWriter(file_path, mode='a') as writer:
    bad_data_df.to_excel(writer, sheet_name='Processed Bad Data', index=False)
