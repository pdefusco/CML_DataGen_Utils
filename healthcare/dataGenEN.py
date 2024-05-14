#****************************************************************************
# (C) Cloudera, Inc. 2020-2024
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

class HealthDataGen:

    '''Class to Generate Banking Data'''

    def __init__(self, spark):
        self.spark = spark

    def biomarkersDataGen(self, shuffle_partitions_requested = 10, partitions_requested = 10, data_rows = 10000):

        # setup use of Faker
        FakerTextUS = FakerTextFactory(locale=['en_US'], providers=[bank])

        # partition parameters etc.
        self.spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        fakerDataspec = (DataGenerator(self.spark, rows=data_rows, partitions=partitions_requested)
                    .withColumn("cd8_perc", "float", minValue=0, maxValue=1, random=True)
                    .withColumn("cd19_perc", "float", minValue=0, maxValue=1, random=True)
                    .withColumn("cd45_abs_count", "float", minValue=1, maxValue=100000, random=True)
                    .withColumn("cd3_perc", "float", minValue=0, maxValue=1, random=True)
                    .withColumn("cd19_abs_count", "float", minValue=1, maxValue=100000, random=True)
                    .withColumn("iga", "float", minValue=1, maxValue=1000, random=True)
                    .withColumn("c3", "float", minValue=1, maxValue=1000, random=True)
                    .withColumn("cd4_abs_count", "float", minValue=1, maxValue=10000, random=True)
                    .withColumn("cd16cd56_perc", "float", minValue=0, maxValue=1, random=True)
                    .withColumn("cd8_abs_count", "float", minValue=1, maxValue=100000, random=True)
                    .withColumn("cd4_ratio_cd8", "float", minValue=0, maxValue=1, random=True)
                    .withColumn("age", "int", minValue=1, maxValue=15, random=True)
                    .withColumn("cd3_abs_count", "float", minValue=1, maxValue=100000, random=True)
                    .withColumn("igm", "float", minValue=1, maxValue=100000, random=True)
                    .withColumn("cd4_perc", "float", minValue=0, maxValue=1, random=True)
                    .withColumn("tige", "float", minValue=1, maxValue=100000, random=True)
                    .withColumn("ch50", "float", minValue=1, maxValue=100000, random=True)
                    .withColumn("c4", "float", minValue=1, maxValue=100000, random=True)
                    .withColumn("cd16cd56_abs_count", "float", minValue=1, maxValue=100000, random=True)
                    .withColumn("allergy_hist", "string", values=["0", "1"], random=True, weights=[7, 3])
                    .withColumn("lung_compl", "string", values=["0", "1"], random=True, weights=[8, 2])
                    .withColumn("gender", "string", values=["0", "1"], random=True, weights=[5, 5])
                    )

        df = fakerDataspec.build()

        df = df.withColumn("allergy_hist", df["allergy_hist"].cast(IntegerType()))
        df = df.withColumn("lung_compl", df["lung_compl"].cast(IntegerType()))
        df = df.withColumn("gender", df["gender"].cast(IntegerType()))

        return df

import cml.data_v1 as cmldata

# Sample in-code customization of spark configurations
#from pyspark import SparkContext
#SparkContext.setSystemProperty('spark.executor.cores', '1')
#SparkContext.setSystemProperty('spark.executor.memory', '2g')

CONNECTION_NAME = "se-aw-mdl"
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

myDG = HealthDataGen(spark)

biomarkersDf = myDG.biomarkersDataGen()
#biomarkersDf.write.format("json").mode("overwrite").save("s3a://goes-se-sandbox01/datalake/pdefusco/biomarkers/jsonData2.json")
