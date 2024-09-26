import os
from dotenv import load_dotenv
from postgresql_client import PostgresSQLClient

# Load environment variables from .env file
load_dotenv()


def main():
    # Create a PostgresSQLClient object with appropriate connection details
    pc = PostgresSQLClient(
        database="k6", user="k6", password="k6", port="5432", host="172.17.0.1"
    )

    # Corrected SQL query to create the table
    create_table_query = """
        DROP TABLE IF EXISTS nyc_taxi_table;

        CREATE TABLE IF NOT EXISTS nyc_taxi_table (
            created TIMESTAMP WITHOUT TIME ZONE,
            VendorID INT,
            tpep_pickup_datetime VARCHAR(30),
            tpep_dropoff_datetime VARCHAR(30),
            passenger_count FLOAT,
            trip_distance FLOAT,
            RatecodeID FLOAT,
            store_and_fwd_flag VARCHAR(2),
            PULocationID INT,
            DOLocationID INT,
            payment_type INT,
            fare_amount FLOAT,
            extra FLOAT,
            mta_tax FLOAT,
            tip_amount FLOAT,
            tolls_amount FLOAT,
            improvement_surcharge FLOAT,
            total_amount FLOAT,
            congestion_surcharge VARCHAR(10),
            airport_fee VARCHAR(10)
);

    """
    try:
        # Execute the SQL query
        pc.execute_query(create_table_query)
        print("Table created successfully.")
    except Exception as e:
        # Handle any errors that occur during table creation
        print(f"Failed to create table with error: {e}")


if __name__ == "__main__":
    main()
