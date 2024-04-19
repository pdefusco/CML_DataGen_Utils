#****************************************************************************
# (C) Cloudera, Inc. 2020-2023
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

import os
import numpy as np
import pandas as pd
from datetime import datetime
import dbldatagen as dg
import dbldatagen.distributions as dist
from dbldatagen import FakerTextFactory, DataGenerator, fakerText
from faker.providers import bank, credit_card, currency
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, \
                              DoubleType, BooleanType, ShortType, \
                              TimestampType, DateType, DecimalType, \
                              ByteType, BinaryType, ArrayType, MapType, \
                              StructType, StructField

class bacDG:

    '''Class to Generate Banking Data'''

    
    def __init__(self, spark):
        self.spark = spark

        
    def bacDataGen(self, data_rows, shuffle_partitions_requested = 5, partitions_requested = 5):
        """
        Method to create synthetic data for BAC
        """

        # setup use of Faker
        FakerTextUS = FakerTextFactory(locale=['es_ES'], providers=[bank])

        # partition parameters etc.
        self.spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        fakerDataspec = (DataGenerator(self.spark, rows=data_rows, partitions=partitions_requested)
                    .withColumn("client_id", "string", minValue=1, maxValue=99999, step=1, prefix='Client_', random=True)
                    .withColumn("m202301", "double", minValue=642.31, maxValue=3837.52, random=True)
                    .withColumn("m202302", "double", minValue=641.50, maxValue=3833.83	, random=True)
                    .withColumn("m202303", "double", minValue=640.21, maxValue=3834.49, random=True)
                    .withColumn("m202304", "double", minValue=642.41	, maxValue=3835.69	, random=True)
                    .withColumn("m202305", "double", minValue=641.53, maxValue=3835.19	, random=True)
                    .withColumn("m202306", "double", minValue=643.24	, maxValue=3836.53, random=True)
                    .withColumn("m202307", "double", minValue=641.92	, maxValue=3838.52, random=True)
                    .withColumn("m202308", "double", minValue=640.90, maxValue=3834.44, random=True)
                    .withColumn("m202309", "double", minValue=642.80, maxValue=3836.54, random=True)
                    .withColumn("m202310", "double", minValue=641.12, maxValue=3835.58, random=True)
                    .withColumn("m202311", "double", minValue=641.70, maxValue=3837.84, random=True)
                    .withColumn("m202312", "double", minValue=641.46, maxValue=3837.41, random=True)
                    .withColumn("m202401", "double", minValue=642.85, maxValue=3837.12, random=True)
                    )

        df = fakerDataspec.build()
        df = df.drop("id_base")

        return df
      
      
    def createDb(self, dbName):
        """
        Method to create database for BAC synthetic data
        """
        try:
            self.spark.sql("CREATE DATABASE {}".format(dbName))
        except Exception as e:
            print("CREATING DATABASE UNSUCCESSFUL")
            print('\n')
            print(f'caught {type(e)}: e')
            print(e)

            
    def saveTbl(self, sparkDf, dbName, tblName):
        """
        Method to create BAC synthetic data table with provided database 
        """
      
        try:
            sparkDf.writeTo("{0}.{1}".format(dbname, tblName)).using("iceberg").tableProperty("write.format.default", "parquet").createOrReplace()
        except Exception as e:
            print("CREATING TABLE UNSUCCESSFUL")
            print('\n')
            print(f'caught {type(e)}: e')
            print(e)

            
import cml.data_v1 as cmldata

# Sample in-code customization of spark configurations
from pyspark import SparkContext
SparkContext.setSystemProperty('spark.executor.cores', '2')
SparkContext.setSystemProperty('spark.executor.memory', '4g')

CONNECTION_NAME = "bacpoccdp"
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

bacDG = bacDG(spark)
bacDb = "CLDR_BAC_TABLES"
bacDG.createDb(bacDb)


#### CREATE TABLE 1: 100k rows
batchCount = 100000
tblName = "data_monitoreo_fake_100k"
sparkDf = bacDG.bacDataGen(batchCount)
sparkDf.writeTo("{0}.{1}".format(bacDb, tblName)).using("iceberg").tableProperty("write.format.default", "parquet").createOrReplace()

#bacDG.saveTbl(sparkDf=sparkDf, dbName=bacDb, tblName=tblName)

#### CREATE TABLE 2: 500k rows
batchCount = 500000
tblName = "data_monitoreo_fake_500k"
sparkDf = bacDG.bacDataGen(batchCount)
sparkDf.writeTo("{0}.{1}".format(bacDb, tblName)).using("iceberg").tableProperty("write.format.default", "parquet").createOrReplace()

#bacDG.saveTbl(sparkDf=sparkDf, dbName=bacDb, tblName=tblName)

#### CREATE TABLE 3: 1M rows
batchCount = 1000000
tblName = "data_monitoreo_fake_1M"
sparkDf = bacDG.bacDataGen(batchCount)
sparkDf.writeTo("{0}.{1}".format(bacDb, tblName)).using("iceberg").tableProperty("write.format.default", "parquet").createOrReplace()

#bacDG.saveTbl(sparkDf=sparkDf, dbName=bacDb, tblName=tblName)


#dataset1.data_monitoreo_fake
# .withColumn("id_base", "int", minValue=1, maxValue=399999, random=False)
# .withColumn("model_line", StringType(), expr="concat('#', id_base)", baseColumn=["id_base"])