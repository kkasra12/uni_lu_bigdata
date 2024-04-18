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

df = spark.read.csv("titanic.csv", header=True, inferSchema=True)
average_age = df.select(mean("Age")).collect()[0][0]
df = df.filter(df.Age > average_age)
df = df.select("Name", "Age")
df = df.withColumn("LastName", split(df["Name"], ",")[0])
df.write.csv("output_Q3a", header=True)
spark.stop()
