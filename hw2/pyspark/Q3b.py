from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, split


spark = (
    SparkSession.builder.appName("Python Spark SQL basic example")
    .master("local[*]")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
)

# Query: “Find the distinct last names of all passengers whose age is greater than the average age of
# all passengers on the Titanic.”
# (b) Implement the above query by issuing only SQL queries from within your Spark session via
# spark.sql("...").


df = spark.read.csv("titanic.csv", header=True, inferSchema=True)
df = df.createOrReplaceTempView("titanic")
df = spark.sql(
    "SELECT Name, Age, split(Name, ',')[0] as LastName "
    "FROM titanic "
    "WHERE Age > (SELECT AVG(Age) FROM titanic)"
)
df.write.csv("output_Q3b", header=True)
spark.stop()
