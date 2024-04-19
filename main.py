import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import logging

# Define a function to extract data from multiple CSV files
def extract_data(file_paths):
    data_frames = []
    for file_path in file_paths:
        data_frames.append(pd.read_csv(file_path))
    return pd.concat(data_frames)

# Define a function to transform the data
def transform_data(data):
    # convert all strings to uppercase
    data = data.applymap(lambda x: x.upper() if isinstance(x, str) else x)
    # Add a new column based on existing columns
    data['new_column'] = data['column1'] + ' ' + data['column2']
    # Remove any rows where a specific column value is missing
    data = data[data['column3'].notna()]
    return data

# Define a function to validate the transformed data
def validate_data(data):
    # Perform any necessary data validation checks here
    # For example, let's check if the sum of a column is equal to a specific value
    total = data['column1'].sum()
    if total != 1000:
        raise ValueError('Total of column1 is not equal to 1000')

# Define a function to load the transformed data to a PostgreSQL database
def load_data(data, conn):
    data.to_sql('table_name', conn, if_exists='replace', index=False)

# Define a function to log messages
def log_message(message):
    logging.info(message)

# Set up the file paths to the CSV files
csv_file_paths = ['file1.csv', 'file2.csv', 'file3.csv']

# Set up logging
logging.basicConfig(filename='etl.log', level=logging.INFO)

# Extract data from the CSV files
try:
    data = extract_data(csv_file_paths)
    log_message('Data extracted successfully')
except Exception as e:
    log_message('Error extracting data: {}'.format(e))
    raise

# Transform the data
try:
    transformed_data = transform_data(data)
    log_message('Data transformed successfully')
except Exception as e:
    log_message('Error transforming data: {}'.format(e))
    raise

# Validate the transformed data
try:
    validate_data(transformed_data)
    log_message('Data validated successfully')
except Exception as e:
    log_message('Error validating data: {}'.format(e))
    raise

# Connect to the PostgreSQL database
engine = create_engine('postgresql://username:password@localhost:5432/database_name')
conn = engine.connect()

# Load the transformed data to the PostgreSQL database
try:
    load_data(transformed_data, conn)
    log_message('Data loaded successfully')
except Exception as e:
    log_message('Error loading data: {}'.format(e))
    raise

# Close the database connection
conn.close()

print("ETL pipeline completed successfully!")
