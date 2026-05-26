from os.path import exists

import pandas as pd
from sqlalchemy import create_engine

path = '../data/processed/yellow_tripdata_2021-01-transformed.parquet'
df = pd.read_parquet(path)

engine = create_engine("postgresql://postgres:postgres@localhost:5432/taxi_db")

df.to_sql("nyc_taxi", engine, if_exists='replace', index=False)

print("Data succeffully loaded into PostgreSQL")
