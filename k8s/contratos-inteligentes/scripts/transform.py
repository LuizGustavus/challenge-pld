import os
import pandas as pd
from datetime import datetime, timedelta

today = datetime.today()
yesterday = today - timedelta(days=1)

# Format the date as a string
yesterday_str = yesterday.strftime("%Y-%m-%d")

# # Read from a CSV file
# csv_filename = F'/contratos-inteligentes/data/raw/extracted_date={yesterday_str}/ethereum_tokens_data.csv'
# df_csv = pd.read_csv(csv_filename)

# Read from a Parquet file
parquet_filename = F'/contratos-inteligentes/data/raw/extracted_date={yesterday_str}/ethereum_tokens_data.parquet.gzip'
df_parquet = pd.read_parquet(parquet_filename)

# Add a new 'block' column to the DataFrame
# df_csv['block'] = df_csv['block_timestamp'].astype(str) + '_' + df_csv['block_hash'].astype(str) + '_' + df_csv['block_number'].astype(str)
df_parquet['block'] = df_parquet['block_timestamp'].astype(str) + '_' + df_parquet['block_hash'].astype(str) + '_' + df_parquet['block_number'].astype(str)

# # Define the local file path where you want to save the transformed data
# output_file_csv = F"/contratos-inteligentes/data/stage/extracted_date={yesterday_str}/ethereum_tokens_data.csv"

# # Extract the directory path from the file path
# directory_path = os.path.dirname(output_file_csv)

# # Create the directory if it doesn't exist
# if not os.path.exists(directory_path):
#     os.makedirs(directory_path)

# # Save the transformed data to a local CSV file
# df_csv.to_csv(output_file_csv, index=False)

# print(f"Data has been transformed and saved to {output_file_csv}")

output_file_parquet = F"/contratos-inteligentes/data/stage/extracted_date={yesterday_str}/ethereum_tokens_data.parquet.gzip"

# Extract the directory path from the file path
directory_path = os.path.dirname(output_file_parquet)

# Create the directory if it doesn't exist
if not os.path.exists(directory_path):
    os.makedirs(directory_path)

df_parquet.to_parquet(output_file_parquet, compression='gzip')

print(f"Data has been transformed and saved to {output_file_parquet}")
