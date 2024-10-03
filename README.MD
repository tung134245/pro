bash stream/run.sh register_connector stream/kafka/configs/postgresql-cdc.json
bash ./flink/data_ingestion/kafka_connect/run.sh register_connector ./flink/data_ingestion/kafka_connect/configs/connect-timescaledb-sink.json

CREATE SCHEMA IF NOT EXISTS mle.nyc_taxi_data
WITH (location = 's3://nyc-taxi-data/');
drop table mle.nyc_taxi_data.pump;
CREATE TABLE IF NOT EXISTS mle.nyc_taxi_data.pump (
	VendorID INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance DOUBLE,
    RatecodeID INTEGER,
    store_and_fwd_flag VARCHAR,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    payment_type INTEGER,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE
) WITH (
  external_location = 's3://nyc-taxi-data/pump',
  format = 'PARQUET'
);