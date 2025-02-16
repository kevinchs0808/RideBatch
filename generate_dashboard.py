import prestodb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from fpdf import FPDF

# Establish Presto connection
conn = prestodb.dbapi.connect(
    host='your-presto-host',
    port=8080,
    user='airflow',
    catalog='hive',
    schema='default',
)
cursor = conn.cursor()

# Define queries
queries = {
    "total_trips_per_day": """
        SELECT DATE(tpep_pickup_datetime) AS trip_date, COUNT(*) AS trip_count
        FROM nyc_taxi_data
        WHERE year = 2025 AND month = 2
        GROUP BY trip_date
        ORDER BY trip_date;
    """,
    "busiest_locations": """
        SELECT nzl.Zone AS pickup_zone, COUNT(*) AS trip_count
        FROM nyc_taxi_data ntd
        JOIN mysql.nyc.taxi_zone_lookup nzl
        ON ntd.PULocationID = nzl.LocationID
        WHERE year = 2025 AND month = 2
        GROUP BY nzl.Zone
        ORDER BY trip_count DESC
        LIMIT 10;
    """,
    "total_revenue_per_day": """
        SELECT DATE(tpep_pickup_datetime) AS trip_date, SUM(Total_amount) AS total_revenue
        FROM nyc_taxi_data
        WHERE year = 2025 AND month = 2
        GROUP BY trip_date
        ORDER BY trip_date;
    """,
    "average_fare_by_payment": """
        SELECT Payment_type, AVG(Fare_amount) AS avg_fare
        FROM nyc_taxi_data
        WHERE year = 2025 AND month = 2
        GROUP BY Payment_type;
    """,
    "hourly_trip_distribution": """
        SELECT HOUR(tpep_pickup_datetime) AS hour, COUNT(*) AS trip_count
        FROM nyc_taxi_data
        WHERE year = 2025 AND month = 2
        GROUP BY hour
        ORDER BY hour;
    """
}

# Execute queries and store results
results = {}
for key, query in queries.items():
    cursor.execute(query)
    results[key] = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

# Close connection
cursor.close()
conn.close()

# Visualization
sns.set(style="whitegrid")
fig, axes = plt.subplots(3, 2, figsize=(15, 10))
fig.suptitle("NYC Taxi Monthly Report")

# Total Trips Per Day
sns.lineplot(data=results['total_trips_per_day'], x='trip_date', y='trip_count', ax=axes[0, 0])
axes[0, 0].set_title("Total Trips Per Day")
axes[0, 0].tick_params(axis='x', rotation=45)

# Busiest Pickup Locations
sns.barplot(data=results['busiest_locations'], x='pickup_zone', y='trip_count', ax=axes[0, 1])
axes[0, 1].set_title("Busiest Pickup Locations")
axes[0, 1].tick_params(axis='x', rotation=45)

# Total Revenue Per Day
sns.lineplot(data=results['total_revenue_per_day'], x='trip_date', y='total_revenue', ax=axes[1, 0])
axes[1, 0].set_title("Total Revenue Per Day")
axes[1, 0].tick_params(axis='x', rotation=45)

# Average Fare by Payment Type
sns.barplot(data=results['average_fare_by_payment'], x='Payment_type', y='avg_fare', ax=axes[1, 1])
axes[1, 1].set_title("Average Fare by Payment Type")

# Hourly Trip Distribution
sns.lineplot(data=results['hourly_trip_distribution'], x='hour', y='trip_count', ax=axes[2, 0])
axes[2, 0].set_title("Hourly Trip Distribution")

# Save the report as a PDF
plt.tight_layout()
plt.savefig("nyc_taxi_report.pdf")

print("Dashboard report generated!")
