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
output_folder = "output_Q3c"
if os.path.exists(output_folder):
    os.system(f"rm -rf {output_folder}")


@pandas_udf("float")
def AgeAverage(ages: pd.Series) -> float:
    avg = ages.fillna(23).mean()
    if VERBOSE:
        print("----------------------------VERBOSE MODE----------------------------")
        print("number of nulls: ", ages.isnull().sum())
        print("avg whitout nulls: ", ages.mean())
        print(avg)
        print("---------------------------------------------------------------------")
    return avg


_ = spark.udf.register("ageAvg", AgeAverage)
df = spark.read.csv("titanic.csv", header=True, inferSchema=True)
df = df.createOrReplaceTempView("titanic")

df = spark.sql(
    "SELECT Name, Age, split(Name, ',')[0] as LastName "
    "FROM titanic "
    "WHERE Age > (SELECT AgeAvg(Age) FROM titanic)"
)

# Continue with the rest of your code
df.write.csv(output_folder, header=True)
spark.stop()
