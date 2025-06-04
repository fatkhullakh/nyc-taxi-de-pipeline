# nyc-taxi-de-pipeline
End-to-end Data Engineering project using NYC Taxi Trip data. Includes ingestion, cleaning, transformation, orchestration, and deployment to cloud.



## 🧪 Data Transformation

In `scripts/transform_data.py`, the following features are engineered:
- `trip_duration_min` = duration in minutes
- `fare_per_km` = total_amount / distance
- `pickup_day_of_week` = name of day from pickup timestamp
- `pickup_hour` = hour of pickup time

Cleaned output saved to: `data/processed/yellow_tripdata_2021-01-transformed.parquet`
