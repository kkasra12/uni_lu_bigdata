import os

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import PandasUDFType, coalesce, mean, pandas_udf, split, udf
from pyspark.sql.types import FloatType

VERBOSE = True
spark = (
    SparkSession.builder.appName("Python Spark SQL basic example")
    .master("local[*]")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
)
output_folder = "output_Q3d"
if os.path.exists(output_folder):
    os.system(f"rm -rf {output_folder}")


df = spark.read.csv("titanic.csv", header=True, inferSchema=True)
df = df.createOrReplaceTempView("titanic")
survived_age_avg = spark.sql(
    "Select avg(Age) from titanic WHERE Survived = 1"
).collect()[0][0]
died_age_avg = spark.sql("Select avg(Age) from titanic where Survived = 0")
died_age_avg = died_age_avg.collect()[0][0]
print(survived_age_avg, died_age_avg, "0000000000000000000000000000000000000000000000")
spark.stop()
# the output is
# 28.343689655172415 30.62617924528302
