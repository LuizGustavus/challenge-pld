import os
import pandas as pd
import psycopg2
from io import StringIO
from datetime import datetime, timedelta

# Retrieve database connection parameters from Kubernetes Secret
db_params = {
    "dbname": os.environ['POSTGRES_DB'],
    "user": os.environ['POSTGRES_USER'],
    "password": os.environ['POSTGRES_PASSWORD'],
    "host": os.environ['POSTGRES_HOST'],
    "port": os.environ['POSTGRES_PORT'],
}

today = datetime.today()
yesterday = today - timedelta(days=1)

# Format the date as a string
yesterday_str = yesterday.strftime("%Y-%m-%d")

# # Read from CSV
# csv_filename = F"/contratos-inteligentes/data/stage/extracted_date={yesterday_str}/ethereum_tokens_data.csv"
# df_csv = pd.read_csv(csv_filename)

# Read from Parquet
parquet_filename = F"/contratos-inteligentes/data/stage/extracted_date={yesterday_str}/ethereum_tokens_data.parquet.gzip"
df_parquet = pd.read_parquet(parquet_filename)

# Function to insert data from a DataFrame into a PostgreSQL table using the PostgreSQL COPY command (df need have save columns order of table)
def insert_dataframe_to_postgres(df, table_name, conn, cursor):
    output = StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cursor.copy_from(output, table_name, sep='\t')
    conn.commit()

try:
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    table_name = 'tokens'

    # # Insert the CSV DataFrame
    # insert_dataframe_to_postgres(df_csv, table_name, conn, cursor)

    # Insert the Parquet DataFrame
    insert_dataframe_to_postgres(df_parquet, table_name, conn, cursor)

except Exception as e:
    print("Error:", e)
finally:
    cursor.close()
    conn.close()
