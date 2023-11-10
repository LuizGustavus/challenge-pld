import os
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta

# Set the path to your service account key file
key_path = '/contratos-inteligentes/config/credentials.json'

# Initialize the BigQuery client with application default credentials
client = bigquery.Client.from_service_account_json(key_path)

# Set your project, dataset, table and column (to use on where clause) names
project_id = "bigquery-public-data"
dataset_id = "crypto_ethereum"
table_id = "tokens"
column_name = 'block_timestamp'

# Calculate the timestamp range for the last day
today = datetime.today()
yesterday = today - timedelta(days=1)
start_time = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
end_time = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)

# Construct the SQL query to retrieve data for the last day
query = f"""
    SELECT *
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE {column_name} >= TIMESTAMP('{start_time}') AND {column_name} <= TIMESTAMP('{end_time}')
"""

# # Define the SQL query to retrieve data from the table
# query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"

# Execute the query and save the results to a Pandas DataFrame
query_job = client.query(query)
df = query_job.result().to_dataframe()

# Format the date as a string
yesterday_str = yesterday.strftime("%Y-%m-%d")

# # Define the local file path where you want to save the data
# output_file_csv = F"/contratos-inteligentes/data/raw/extracted_date={yesterday_str}/ethereum_tokens_data.csv"

# # Extract the directory path from the file path
# directory_path = os.path.dirname(output_file_csv)

# # Create the directory if it doesn't exist
# if not os.path.exists(directory_path):
#     os.makedirs(directory_path)

# # Save the data to a local CSV file
# df.to_csv(output_file_csv, index=False)

# print(f"Data has been extracted and saved to {output_file_csv}")

output_file_parquet = F"/contratos-inteligentes/data/raw/extracted_date={yesterday_str}/ethereum_tokens_data.parquet.gzip"

# Extract the directory path from the file path
directory_path = os.path.dirname(output_file_parquet)

# Create the directory if it doesn't exist
if not os.path.exists(directory_path):
    os.makedirs(directory_path)

df.to_parquet(output_file_parquet, compression='gzip')

print(f"Data has been extracted and saved to {output_file_parquet}")
