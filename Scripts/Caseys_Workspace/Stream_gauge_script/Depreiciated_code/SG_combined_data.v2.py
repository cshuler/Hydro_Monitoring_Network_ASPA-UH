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

# New Header Structure based on your example
NEW_HEADER = [
    'Date/ Time', 'Abs Pres (psi)', 'Temp°', 'Atmospheric Abs Pres (psi)', 'Notes'
]

# Adjust the column mapping as needed
variable_mapping = {
    'Date/ Time': ['Date/ Time', 'Datetime'],
    'Abs Pres (psi)': ['Abs Pres (psi)', 'Pressure'],
    'Temp°': ['Temp°', 'Temperature'],
    'Atmospheric Abs Pres (psi)': ['Atmospheric Abs Pres (psi)', 'Atmospheric Pressure'],
    'Notes': ['Notes']
}

# Function to clean and map the data
def clean_and_map_data(df):
    df_cleaned = pd.DataFrame()

    for new_col in NEW_HEADER:
        mapped_columns = variable_mapping.get(new_col, [])
        found_col = None

        # Find the first matching column
        for col in mapped_columns:
            if col in df.columns:
                found_col = col
                break
        
        # Add the column to the cleaned DataFrame
        if found_col:
            df_cleaned[new_col] = df[found_col]
        else:
            # If the column is missing, add a column filled with NaN
            df_cleaned[new_col] = pd.NA

    return df_cleaned

# Function to process the dataset
def process_dataset(file_path):
    try:
        print(f"Processing file: {file_path}")
        df = pd.read_excel(file_path)

        # Clean and map the data
        df_cleaned = clean_and_map_data(df)
        return df_cleaned

    except Exception as e:
        logging.error(f"Failed to process file {file_path}: {e}")
        print(f"Error processing {file_path}: {e}")
        return None

# Function to process multiple files and save to Excel
def process_multiple_files(input_dir, output_file):
    combined_df = pd.DataFrame()

    for root, dirs, files in os.walk(input_dir):
        for filename in files:
            if filename.endswith(".xlsx") or filename.endswith(".xls"):
                file_path = os.path.join(root, filename)
                df_cleaned = process_dataset(file_path)

                if df_cleaned is not None:
                    combined_df = pd.concat([combined_df, df_cleaned], ignore_index=True)
                    logging.info(f"Successfully processed: {file_path}")

    # Save the combined data to an Excel file
    combined_df.to_excel(output_file, index=False, engine='openpyxl')
    print(f"Data saved to {output_file}")

# Main function
def main():
    input_directory = r'C:\Users\ctebe\OneDrive\Desktop\SG'  # Updated directory
    output_file = r'C:\Users\ctebe\OneDrive\Desktop\SG_combined_output.xlsx'  # Output file path
    process_multiple_files(input_directory, output_file)

if __name__ == "__main__":
    main()
