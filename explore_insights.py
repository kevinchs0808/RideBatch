from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, hour, dayofweek, date_format, when, expr
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Initialize Spark Session
spark = SparkSession.builder.appName("NYC_Taxi_Analysis").getOrCreate()

# Load Data
rides_df = spark.read.csv("data/yellow_tripdata_2019-01.csv", header=True, inferSchema=True)
zones_df = spark.read.csv("data/taxi+_zone_lookup.csv", header=True, inferSchema=True)

# Alias the `zones_df`
zones_df_alias = zones_df.alias("zones")

# Join on PULocationID (Pickup Location)
rides_df = rides_df.alias("rides").join(
    zones_df_alias.withColumnRenamed("Borough", "PU_Borough").withColumnRenamed("Zone", "PU_Zone"),
    col("rides.PULocationID") == col("zones.LocationID"),
    "left"
).drop(col("zones.LocationID"))  # Drop duplicate column

# Alias zones_df again for Dropoff Location
zones_df_alias2 = zones_df.alias("zones2")

# Join on DOLocationID (Dropoff Location)
rides_df = rides_df.join(
    zones_df_alias2.withColumnRenamed("Borough", "DO_Borough").withColumnRenamed("Zone", "DO_Zone"),
    col("rides.DOLocationID") == col("zones2.LocationID"),
    "left"
).drop(col("zones2.LocationID"))  # Drop duplicate column

# Extract Date and Day of Week
rides_df = rides_df.withColumn("daily_date", date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd"))
rides_df = rides_df.withColumn("hour", hour("tpep_pickup_datetime"))
rides_df = rides_df.withColumn("day_of_week", date_format("tpep_pickup_datetime", "EEEE"))  # Monday-Sunday
rides_df = rides_df.withColumn(
    "day_of_week_order", 
    expr(
        """CASE 
            WHEN day_of_week = 'Monday' THEN 1
            WHEN day_of_week = 'Tuesday' THEN 2
            WHEN day_of_week = 'Wednesday' THEN 3
            WHEN day_of_week = 'Thursday' THEN 4
            WHEN day_of_week = 'Friday' THEN 5
            WHEN day_of_week = 'Saturday' THEN 6
            WHEN day_of_week = 'Sunday' THEN 7
        END"""
    )
)

rides_df = rides_df.where((col("daily_date") >= "2019-01-01") & (col("daily_date") <= "2019-01-31"))

# Aggregate Metrics
# 1️⃣ Heatmap: Number of Trips vs. Days of the Week
rides_df = rides_df.orderBy("day_of_week_order")
day_trip_counts = rides_df.groupBy("hour", "day_of_week_order").count().withColumnRenamed("count", "trip_count")
heatmap_df = day_trip_counts.toPandas().pivot(index="day_of_week_order", columns="hour", values="trip_count")

day_mapping = {
    1: "Monday",
    2: "Tuesday",
    3: "Wednesday",
    4: "Thursday",
    5: "Friday",
    6: "Saturday",
    7: "Sunday"
}

heatmap_df.index = heatmap_df.index.map(day_mapping)

# 2️⃣ Top 10 Starting Zones Generating Highest Revenue
revenue_by_zone = rides_df.groupBy("PU_Zone").agg(sum("total_amount").alias("total_revenue"))
top_revenue_zones = revenue_by_zone.orderBy(col("total_revenue").desc()).limit(10).toPandas()

# 3️⃣ Line Plot: Number of Trips (Daily) by Destination Service Zone
service_zone_trips = rides_df.groupBy("daily_date", "day_of_week_order", "DO_Borough").agg(count("tpep_pickup_datetime").alias("trip_count"))
service_zone_trips_pd = service_zone_trips.toPandas()

# 4️⃣ Top 10 Starting Zones by Total Passengers per Day (Stacked Weekday/Weekend)
rides_df = rides_df.withColumn("is_weekend", when(col("day_of_week").isin('Saturday', 'Sunday'), "Weekend").otherwise("Weekday"))

## Aggregate trip count by city (PU_Zone) and is_weekend
trips_by_zone = rides_df.groupBy("PU_Zone", "is_weekend").agg(
    count("*").alias("total_trips")
)

## Compute overall trip count per PU_Zone
overall_trips_by_zone = trips_by_zone.groupBy("PU_Zone").agg(
    sum("total_trips").alias("overall_total_trips")
)

## Get top 10 PU_Zone by total number of trips
top_trip_zones = overall_trips_by_zone.orderBy(col("overall_total_trips").desc()).limit(10)

## Join back to get breakdown of weekday vs. weekend
top_trip_zones_df = top_trip_zones.join(trips_by_zone, "PU_Zone", "inner").toPandas()

## Pivot Data for Visualization
passenger_pivot_df = top_trip_zones_df.pivot(index="PU_Zone", columns="is_weekend", values="total_trips").fillna(0)

## Sort the index based on overall_total_trips
passenger_pivot_df = passenger_pivot_df.loc[top_trip_zones_df.sort_values("overall_total_trips", ascending=False)["PU_Zone"].unique()]

## 🎨 Combine Visualizations into One Dashboard
fig, axes = plt.subplots(2, 2, figsize=(14, 12))

# 1️⃣ Heatmap: Number of Trips vs. Days of the Week
sns.heatmap(heatmap_df, cmap="coolwarm", ax=axes[0, 0])
axes[0, 0].set_title("Heatmap of Trips by Day of the Week")

# 2️⃣ Bar Chart: Top 10 Starting Zones by Revenue
sns.barplot(x="total_revenue", y="PU_Zone", data=top_revenue_zones, ax=axes[0, 1], palette="Blues_r")
axes[0, 1].set_title("Top 10 Revenue Generating Pickup Zones")

# 3️⃣ Line Plot: Trips by Destination Service Zone

## Convert daily_date to datetime if not already
#service_zone_trips_pd["daily_date"] = pd.to_datetime(service_zone_trips_pd["daily_date"])

## Extract Mondays
#mondays = service_zone_trips_pd[service_zone_trips_pd["day_of_week"] == 1]["daily_date"].unique()

#print(mondays)

## Create Line Plot
sns.lineplot(x="daily_date", y="trip_count", hue="DO_Borough", data=service_zone_trips_pd, ax=axes[1, 0])

service_zone_trips_pd = service_zone_trips_pd.reset_index()

mondays = service_zone_trips_pd[service_zone_trips_pd['day_of_week_order'] == 1]['daily_date'].unique()

## Format the x-axis to show only Mondays
axes[1, 0].set_xticks(mondays)
axes[1, 0].set_xticklabels(pd.to_datetime(mondays).strftime('%Y-%m-%d'), rotation=90)

## Add dashed vertical lines at each Monday
for monday in mondays:
    axes[1, 0].axvline(monday, color='gray', linestyle='dashed', alpha=0.6)

## Title and Labels
axes[1, 0].set_title("Daily Trips by Destination Service Zone")
axes[1, 0].set_xlabel("Date")
axes[1, 0].set_ylabel("Trip Count")

# 4️⃣ Stacked Bar Chart: Top Pickup Zones by Passengers (Weekday vs Weekend)

passenger_pivot_df.plot(kind="bar", stacked=True, ax=axes[1, 1], figsize=(12, 6), color=["blue", "orange"])
plt.title("Top 10 Pickup Zones by Total Trips (Breakdown by Weekday/Weekend)")
plt.xlabel("Pickup Zone")
plt.ylabel("Number of Trips")
plt.legend(title="Day Type", labels=["Weekday", "Weekend"])
plt.xticks(rotation=90)
axes[1, 1].set_title("Top Pickup Zones by Passengers (Weekday vs Weekend)")

# Adjust Layout & Save
plt.tight_layout()
plt.savefig("result/nyc_taxi_dashboard.png", dpi=300)
plt.show()
