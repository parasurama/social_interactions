#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" 
Author: Prasanna Parasurama
"""
from pyspark.sql.session import SparkSession
import shapely.speedups
from geospark.register import GeoSparkRegistrator, upload_jars
from geospark.utils import KryoSerializer, GeoSparkKryoRegistrator
from pyspark.sql.functions import broadcast, countDistinct, monotonically_increasing_id, abs, date_trunc, count
from pyspark.sql import functions as f
import pyproj
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

from functools import partial
from shapely.ops import transform
from shapely.geometry import Point, Polygon


def geodesic_point_buffer(lat, lon, radius):
    """
    get circle around a lat lon given a radius in meters
    :param lat:
    :param lon:
    :param radius: in meters
    :return:
    """
    proj_wgs84 = pyproj.Proj(init='epsg:4326')
    # Azimuthal equidistant projection
    aeqd_proj = '+proj=aeqd +lat_0={lat} +lon_0={lon} +x_0=0 +y_0=0'
    project = partial(
        pyproj.transform,
        pyproj.Proj(aeqd_proj.format(lat=lat, lon=lon)),
        proj_wgs84)
    buf = Point(0, 0).buffer(radius)  # distance in metres
    points_list = transform(project, buf).exterior.coords[:]
    return points_list


shapely.speedups.enable()
upload_jars()

spark = SparkSession.builder \
        .appName("interaction_count") \
        .config("spark.serializer", KryoSerializer.getName) \
        .config("spark.kryo.registrator", GeoSparkKryoRegistrator.getName) \
        .getOrCreate()

sc = spark.sparkContext
GeoSparkRegistrator.registerAll(spark)

# all pings
df = spark.read.parquet("/user/pp1994/venpath/social_interaction/san_francisco.parquet")
df = df.filter(df['ad_id'] != "00000000-0000-0000-0000-000000000000")
df = df.withColumn("id", monotonically_increasing_id())
df = df.withColumn("timestamp", date_trunc("minute", "timestamp"))
df.createOrReplaceTempView("pings")

df = spark.sql(
        "SELECT *, ST_Point(CAST(pings.lon AS Decimal(24,20)), CAST(pings.lat AS Decimal(24,20))) AS geom FROM pings")
df.createOrReplaceTempView("pings")


# sample individuals 1% sample
sample_ids = df.select("ad_id").distinct().sample(withReplacement=False, fraction=0.01).select("ad_id").toPandas()['ad_id'].values
df_sample = df.filter(df['ad_id'].isin(sample_ids.tolist()))
df_sample.createOrReplaceTempView("sample")

# distance is in lat/lon units and depends on lat/lon of the region
# 0.00002 is ~ 2 meters in san francisco
df_int = spark.sql("""
SELECT * 
FROM sample, pings 
WHERE ST_DISTANCE(sample.geom, pings.geom) <= 0.00002
AND sample.timestamp = pings.timestamp
AND sample.ad_id != pings.ad_id
""")


df_count = df_int.groupby('sample.ad_id').agg(countDistinct("pings.ad_id").alias("uniq_count"))
