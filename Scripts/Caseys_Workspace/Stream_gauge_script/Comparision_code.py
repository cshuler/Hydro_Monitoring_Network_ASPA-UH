import pandas as pd

# Load the file with the headers
file_path = '/mnt/data/Headers.xlsx'
headers_df = pd.read_excel(file_path)

# Display the first row which is the standardized header Brandon provided
standardized_header = headers_df.iloc[0]

# Display the collected standardized headers for review
standardized_header
