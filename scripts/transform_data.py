import pandas as pd
import numpy as np
import os

path = '../data/clean/yellow_tripdata_2021-01-cleaned.parquet'

df = pd.read_parquet(path)

print("Before filetring", len(df))
# duration = df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']
# print(duration)

df['trip_duration_min'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60

df['fare_per_km'] = df['total_amount'] / df['trip_distance']
df['pickup_day_of_week'] = df['tpep_pickup_datetime'].dt.day_name()
df['pickup_hour'] = df['tpep_pickup_datetime'].dt.hour


# .loc is usefull when I want to modify specific columns
df = df.loc[
    (df['trip_duration_min'] <= 120) &
    (df['fare_per_km'] <= 100) &
    (df['trip_distance'] <= 100) &
    (df['total_amount'] <= 500)
]

print("After filetring", len(df))

print(df.head())
df.to_parquet("../data/processed/yellow_tripdata_2021-01-transformed.parquet", index=False)



# print(df.describe())
# VendorID
# tpep_pickup_datetime
# tpep_dropoff_datetime
# passenger_count
# trip_distance
# RatecodeID...mta_tax
# tip_amount
# tolls_amount
# improvement_surcharge
# total_amount
# congestion_surcharge
