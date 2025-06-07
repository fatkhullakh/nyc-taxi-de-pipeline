import duckdb

#df = duckdb.sql(r"SELECT * FROM 'C:\DE\nyc-taxi-de-pipeline\nyc-taxi-de-pipeline\data\processed\yellow_tripdata_2021-01-transformed.parquet' LIMIT 5").df()
path = "C:/DE/nyc-taxi-de-pipeline/nyc-taxi-de-pipeline/data/processed/yellow_tripdata_2021-01-transformed.parquet"

# r"" tells python treat the string literally, don’t escape anything.
df = duckdb.sql(r"DESCRIBE 'C:\DE\nyc-taxi-de-pipeline\nyc-taxi-de-pipeline\data\processed\yellow_tripdata_2021-01-transformed.parquetC:\DE\nyc-taxi-de-pipeline\nyc-taxi-de-pipeline\data\processed\yellow_tripdata_2021-01-transformed.parquet'").df()
#               column_name column_type null   key default extra
# 0                VendorID      BIGINT  YES  None    None  None
# 1    tpep_pickup_datetime   TIMESTAMP  YES  None    None  None
# 2   tpep_dropoff_datetime   TIMESTAMP  YES  None    None  None
# 3         passenger_count      DOUBLE  YES  None    None  None
# 4           trip_distance      DOUBLE  YES  None    None  None
# 5              RatecodeID      DOUBLE  YES  None    None  None
# 6      store_and_fwd_flag     VARCHAR  YES  None    None  None
# 7            PULocationID      BIGINT  YES  None    None  None
# 8            DOLocationID      BIGINT  YES  None    None  None
# 9            payment_type      BIGINT  YES  None    None  None
# 10            fare_amount      DOUBLE  YES  None    None  None
# 11                  extra      DOUBLE  YES  None    None  None
# 12                mta_tax      DOUBLE  YES  None    None  None
# 13             tip_amount      DOUBLE  YES  None    None  None
# 14           tolls_amount      DOUBLE  YES  None    None  None
# 15  improvement_surcharge      DOUBLE  YES  None    None  None
# 16           total_amount      DOUBLE  YES  None    None  None
# 17   congestion_surcharge      DOUBLE  YES  None    None  None
# 18      trip_duration_min      DOUBLE  YES  None    None  None
# 19            fare_per_km      DOUBLE  YES  None    None  None
# 20     pickup_day_of_week     VARCHAR  YES  None    None  None
# 21            pickup_hour     INTEGER  YES  None    None  None



# Top 10 Longest trips (by trip_distance)

df = duckdb.sql(f"SELECT trip_distance, total_amount, passenger_count FROM '{path}' ORDER BY trip_distance DESC LIMIT 10").df()

#    trip_distance  total_amount  passenger_count
# 0          95.50          6.43              1.0
# 1          93.47        258.06              1.0
# 2          90.44        100.30              3.0
# 3          88.13        396.30              1.0
# 4          87.70        183.30              1.0
# 5          86.80          0.31              1.0
# 6          85.27        377.92              1.0
# 7          82.66        230.30              1.0
# 8          81.46        270.17              1.0
# 9          80.20        340.67              1.0



# How many trips happened each hour?

df = duckdb.sql(f"SELECT pickup_hour, COUNT(*) AS trip_count FROM '{path}' GROUP BY pickup_hour ORDER BY pickup_hour").df()
#     pickup_hour  trip_count
# 0             0       10804
# 1             1        7036
# 2             2        3942
# 3             3        2244
# 4             4        2226
# 5             5        6066
# 6             6       20677
# 7             7       39445
# 8             8       60715
# 9             9       65416
# 10           10       72388
# 11           11       78607
# 12           12       86684
# 13           13       91906
# 14           14      100277
# 15           15      101496
# 16           16       95743
# 17           17       94709
# 18           18       86446
# 19           19       64684
# 20           20       43955
# 21           21       34247
# 22           22       28353
# 23           23       17206


df = duckdb.sql(
    # f"" is for inserting the value of the variable "path" into the SQL string
    f"SELECT PULocationID, DOLocationID, COUNT(*) AS trip_count FROM '{path}' GROUP BY PULocationID, DOLocationID ORDER BY trip_count DESC LIMIT 10"
).df()

print(df)

