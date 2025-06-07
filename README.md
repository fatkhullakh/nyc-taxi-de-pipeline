# nyc-taxi-de-pipeline
End-to-end Data Engineering project using NYC Taxi Trip data. Includes ingestion, cleaning, transformation, orchestration, and deployment to cloud.



## 🧪 Data Transformation

In `scripts/transform_data.py`, the following features are engineered:
- `trip_duration_min` = duration in minutes
- `fare_per_km` = total_amount / distance
- `pickup_day_of_week` = name of day from pickup timestamp
- `pickup_hour` = hour of pickup time

Cleaned output saved to: `data/processed/yellow_tripdata_2021-01-transformed.parquet`


## 🛠️ PostgreSQL Integration

After cleaning, transforming, and filtering the NYC Taxi data, the final dataset is loaded into a local PostgreSQL database.

### Table: `nyc_taxi`

| Column               | Description                       |
|----------------------|-----------------------------------|
| trip_distance        | Distance in miles                 |
| fare_amount          | Fare charged                      |
| total_amount         | Full payment incl. tips/surcharges|
| trip_duration_min    | Trip duration in minutes          |
| fare_per_km          | Cost efficiency metric            |
| pickup_day_of_week   | Day of the week (e.g. Monday)     |
| pickup_hour          | Hour of day                       |

## Sample Query:
```sql
SELECT pickup_day_of_week, COUNT(*) 
FROM nyc_taxi 
GROUP BY pickup_day_of_week 
ORDER BY COUNT(*) DESC;

## 🦆 DuckDB Analytics (Local SQL on Parquet)

This project uses [DuckDB](https://duckdb.org/) to query Parquet files directly, simulating cloud warehouse behavior like AWS Athena or BigQuery — but locally.

### 🔎 Key Queries Performed

1. **Top 10 Longest Trips**
```sql
SELECT trip_distance, total_amount, passenger_count
FROM 'data/processed/yellow_tripdata_2021-01-transformed.parquet'
ORDER BY trip_distance DESC
LIMIT 10;
