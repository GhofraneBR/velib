# Databricks notebook source
import requests

# Télécharger les données de stations Vélib’ en temps réel (format JSON)
url = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"
response = requests.get(url)
data = response.json()

# Extraire la liste des stations
stations = data['data']['stations']


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Créer un Spark DataFrame
schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("capacity", IntegerType(), True)
])

df = spark.createDataFrame(stations, schema=schema)
df.show(5)


# COMMAND ----------

# Convertir en Pandas pour créer la carte
stations_pd = df.toPandas()
stations_pd.head()


# COMMAND ----------

# MAGIC %pip install folium
# MAGIC

# COMMAND ----------

import folium
from folium.plugins import MarkerCluster
from IPython.display import HTML

# Crée la carte centrée sur Paris
map_velib = folium.Map(location=[48.8566, 2.3522], zoom_start=13)
marker_cluster = MarkerCluster().add_to(map_velib)

# Ajoute chaque station
for station in stations_pd.to_dict(orient='records'):
    folium.Marker(
        location=[station['lat'], station['lon']],
        popup=f"{station['name']} (Capacité: {station['capacity']})"
    ).add_to(marker_cluster)

# Affiche la carte dans le notebook
displayHTML(map_velib._repr_html_())


# COMMAND ----------

# 1. Crée la base de données (comme CREATE DATABASE en Hive)
spark.sql("CREATE DATABASE IF NOT EXISTS velib_hive")

# 2. Écris ton DataFrame dans une table gérée (Delta), à la place d'un JSON dans HDFS
df.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("velib_hive.raw_stations")


# COMMAND ----------

# MAGIC %sql
# MAGIC USE velib_hive;
# MAGIC SHOW TABLES;
# MAGIC SELECT * FROM raw_stations LIMIT 5;
# MAGIC

# COMMAND ----------

stations_big = spark.table("workspace.velib_hive.raw_stations").toPandas()

import folium
m2 = folium.Map(location=[48.8566, 2.3522], zoom_start=13)
for _, r in stations_big.iterrows():
    folium.Marker(
        [r.lat, r.lon],
        popup=f"{r.name} (capacité : {r.capacity})"
    ).add_to(m2)

displayHTML(m2._repr_html_())