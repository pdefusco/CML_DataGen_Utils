from pyspark import SparkContext
from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
import os, warnings, sys, logging
import pandas as pd
import numpy as np
from datetime import date
import cml.data_v1 as cmldata
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark import StorageLevel
import pandas as pd
from sedona.spark import *
from sedona.core.geom.envelope import Envelope

config = SedonaContext.builder() .\
    config('spark.jars.packages',
           'org.apache.sedona:sedona-spark-3.0_2.12:1.5.1,'
           'org.datasyslab:geotools-wrapper:1.5.1-28.2,'
           'uk.co.gresearch.spark:spark-extension_2.12:2.11.0-3.4'). \
    config('spark.jars.repositories', 'https://artifacts.unidata.ucar.edu/repository/unidata-all'). \
    getOrCreate()


states_wkt = sedona.read.option("delimiter", "\t")\
                    .option("header", "false")\
                    .csv("data/boundary-each-state.tsv")\
                    .toDF("s_name","s_bound")

states = states_wkt.selectExpr("s_name", "ST_GeomFromWKT(s_bound) as s_bound")
states.createOrReplaceTempView("states")

iotDf = sedona.read.option("delimiter", ",")\
            .option("header", "true")\
            .csv("data/iot_data.csv")

iotDf.createOrReplaceTempView("IOT_FLEET_DATA")
