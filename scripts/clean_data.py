import pandas as pd
import numpy as np
import os

parquet_path = '../data/raw/yellow_tripdata_2021-01.parquet'
output_path = '../data/clean/yellow_tripdata_2021-01-cleaned.parquet'

df = pd.read_parquet(parquet_path)

print(df.head())
print(f"Rows before cleaning: {len(df)}")


df = df.dropna(subset=['passenger_count', 'RatecodeID', 'store_and_fwd_flag', 'congestion_surcharge',])

df = df.drop(columns=['airport_fee'])

df = df[
    (df['trip_distance'] > 0) &
    (df['fare_amount'] > 0) &
    (df['passenger_count'] > 0)
]

df.to_parquet(output_path, index=False)

print(df.head())
print(f"Rows after cleaning: {len(df)}")



