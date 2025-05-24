# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType
import matplotlib.pyplot as plt
from pyspark.sql.functions import col, sqrt, min as spark_min
from pyspark.sql.functions import lit
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler

# COMMAND ----------

crime_dataset = sc.textFile("dbfs:/FileStore/shared_uploads/padeos1@lsu.edu/Crimes___2001_to_Present_20240928.csv")
pharmacy_dataset = sc.textFile("dbfs:/FileStore/shared_uploads/padeos1@lsu.edu/Pharmacy_Status_20240928.csv")

# COMMAND ----------

def is_float(value):
    try:
        x = float(value)
        return x
    except ValueError:
        return 0

# COMMAND ----------

crime_split = crime_dataset.map(lambda line: line.split(","))
crime_header = crime_split.first()
longitude = crime_header.index("Longitude")
latitude = crime_header.index("Latitude")
crime_data = crime_split.filter(lambda row: row != crime_header and row[latitude] and row[latitude])
crime_y = crime_data.map(lambda row: is_float(row[latitude])).filter(lambda row: row % 1 != 0)
crime_x = crime_data.map(lambda row: is_float(row[longitude])).filter(lambda row: row % 1 != 0)

# COMMAND ----------

pharm_split = pharmacy_dataset.map(lambda line: line.split(","))
pharm_header = pharm_split.first()
geo_index = pharm_header.index("New Georeferenced Column")
pharm_data = pharm_split.filter(lambda row: row != pharm_header)
pharm_xy = pharm_data.map(lambda row: row[geo_index - 1])
parsed_xy = pharm_xy.map(lambda coord: coord.replace("POINT (", "").replace(")", "").split())
pharm_x = parsed_xy.map(lambda coord: float(coord[0]) if len(coord) == 2 else None)
pharm_y = parsed_xy.map(lambda coord: float(coord[1]) if len(coord) == 2 else None)

# COMMAND ----------

schema = StructType([
    StructField("crime_x", FloatType(), True),
    StructField("crime_y", FloatType(), True),
    StructField("pharm_x", FloatType(), True),
    StructField("pharm_y", FloatType(), True)
])

min_length = 10000
crime_x = crime_x.zipWithIndex().filter(lambda x: x[1] < min_length).map(lambda x: x[0])
crime_y = crime_y.zipWithIndex().filter(lambda x: x[1] < min_length).map(lambda x: x[0])

# COMMAND ----------

crime_xy = crime_x.zip(crime_y)
pharm_xy = pharm_x.zip(pharm_y)
crime_df = spark.createDataFrame(zipped_crime, ["crime_x", "crime_y"])
crime_df = crime_df.na.drop(subset = ["crime_x", "crime_y"])
pharmacy_df = spark.createDataFrame(zipped_pharm, ["pharm_x", "pharm_y"])
pharmacy_df = pharmacy_df.na.drop(subset = ["pharm_x", "pharm_y"])

cross_df = crime_df.crossJoin(pharmacy_df)
distance_df = cross_df.withColumn("distance", sqrt(
    (col("crime_x") - col("pharm_x"))**2 + 
    (col("crime_y") - col("pharm_y"))**2
))
min_distance_df = distance_df.groupBy("crime_x", "crime_y").agg(
    spark_min("distance").alias("min_distance")
)
crime_with_distance_df = crime_df.join(
    min_distance_df, 
    (crime_df.crime_x == min_distance_df.crime_x) & (crime_df.crime_y == min_distance_df.crime_y),
    "left"
).drop(min_distance_df.crime_x).drop(min_distance_df.crime_y)


# COMMAND ----------

crime_with_distance_df = crime_with_distance_df.withColumn("type", lit("crime"))
pharmacy_df = pharmacy_df.withColumn("min_distance", lit(0))
pharmacy_df = pharmacy_df.withColumn("type", lit("pharmacy"))
combined_df = crime_with_distance_df.union(pharmacy_df)

# COMMAND ----------

assembler = VectorAssembler(inputCols=["crime_x", "crime_y", "min_distance"], outputCol="features")
feature_df = assembler.transform(combined_df)
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
scaled_df = scaler.fit(feature_df).transform(feature_df)

# COMMAND ----------

kmeans = KMeans(k=8, seed=1, featuresCol="features")
model = kmeans.fit(feature_df)
clustered_df = model.transform(feature_df)
clustered_df.select("crime_x", "crime_y", "type", "min_distance", "prediction").show()


# COMMAND ----------

plot_data = clustered_df.select("crime_x", "crime_y", "type", "prediction").collect()

x_coords = [row["crime_x"] for row in plot_data]
y_coords = [row["crime_y"] for row in plot_data]
labels = [row["prediction"] for row in plot_data]
types = [row["type"] for row in plot_data]

markers = {'crime': 'o', 'pharmacy': 'x'}
colors = {'crime': 'blue', 'pharmacy': 'red'}

for (x, y, label, point_type) in zip(x_coords, y_coords, labels, types):
    plt.scatter(x, y, label=label, marker=markers[point_type], color=colors[point_type])

plt.xlabel("X Coordinate")
plt.ylabel("Y Coordinate")
plt.title("Crime vs Pharmacy Data")
plt.legend(loc="lower right")
plt.show()

