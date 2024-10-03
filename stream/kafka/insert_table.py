import os
import random
from datetime import datetime
from time import sleep
import pandas as pd
from dotenv import load_dotenv
from postgresql_client import PostgresSQLClient

load_dotenv()

TABLE_NAME = "nyc_taxi_table"
FOLDER_NAME = "/home/tung/Downloads/DE_K3_M2/nyc_taxi_data"

def main():
    pc = PostgresSQLClient(
        database="k6",
        user="k6",
        password="k6",
        host="localhost",
    )

    # Get all columns from the nyc_taxi table
    try:
        columns = pc.get_columns(table_name=TABLE_NAME)
        print(columns)
        print(len(columns))
    except Exception as e:
        print(f"Failed to get schema for table with error: {e}")

    # Loop through Parquet files in nyc_taxi_data directory
    for filename in os.listdir(FOLDER_NAME):
        if filename.endswith(".parquet") and filename.startswith("yellow"):
            filepath = os.path.join(
                FOLDER_NAME, filename
            )
            print(f"Inserting: {filename}")
            # Read Parquet data
            data = pd.read_parquet(filepath)
            current_datetime = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            data["tpep_pickup_datetime"] = data["tpep_pickup_datetime"].astype(str)
            data["tpep_dropoff_datetime"] = data["tpep_dropoff_datetime"].astype(str)
            data = data.fillna("NULL")
            try:
                for index, row in data.iterrows():
                    values = [current_datetime] + list(
                        row
                    )  # Convert row to tuple for SQL query
                    insert_query = f"""
                        INSERT INTO nyc_taxi_table VALUES {tuple(values)};
                    """
                    pc.execute_query(insert_query)
                    
            except Exception as e:
                print(f"Error: {e}")
            print(f"Inserted {filename}")


if __name__ == "__main__":
    main()
