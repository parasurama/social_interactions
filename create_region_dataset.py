#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" 
Author: Prasanna Parasurama
"""

from pyspark.sql.session import SparkSession
import geopandas as gpd
from shapely.geometry import Polygon

spark = SparkSession.builder.getOrCreate()

# get bounding box
# https://boundingbox.klokantech.com/
# san francisco
envelope = [[-122.516952558,37.690431822],[-122.3771726833,37.690431822],[-122.3771726833,37.8117899711],[-122.516952558,37.8117899711],[-122.516952558,37.690431822]]

lon_min = min([x[0] for x in envelope])
lon_max = max([x[0] for x in envelope])
lat_min = min([x[1] for x in envelope])
lat_max = max([x[1] for x in envelope])


df = spark.read.parquet("/user/pp1994/venpath/pings/year=2017/month=7/*/*.parquet")
df = df.filter((df['lat'].between(lat_min, lat_max)) & (df['lon'].between(lon_min, lon_max)))
df.coalesce(100).write.parquet("/user/pp1994/venpath//social_interaction/san_francisco.parquet")



