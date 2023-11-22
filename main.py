from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# Create a Spark session
spark = SparkSession.builder.appName("UniqueValues").getOrCreate()

# Set the log level to WARN (or ERROR for even less information) for seeing output
spark.sparkContext.setLogLevel("ERROR")

# # Print the schema
# schema = StructType([
#     StructField("PassengerId", IntegerType(), True),
#     StructField("Survived", IntegerType(), True),
#     StructField("Pclass", StringType(), True),
#     StructField("Name", StringType(), True),
#     StructField("Sex", StringType(), True),
#     StructField("Age", IntegerType(), True),
#     StructField("SibSp", IntegerType(), True),
#     StructField("Parch", IntegerType(), True),
#     StructField("Ticket", StringType(), True),
#     StructField("Fare", IntegerType(), True),
# ])

# Read data into a Pandas DataFrame
spark_df = spark.read.csv("/home/axat/work/pySpark/tested.csv" , header=True, inferSchema=True)

# dictionary to store count of unique values
countMap = {}
# Iterate through each column and print unique values
for column_name in spark_df.columns:
    unique_value_count = spark_df.select(column_name).distinct().count()
    countMap[column_name] = unique_value_count

print("-------------countMap-------------------")    
print(countMap)
print("-------------countMap-------------------")

#check if there were any survivours if there was any check if they were male or female and give a count of male and female survived
# Check if there were any survivors
survivors = spark_df.filter(col("Survived") == 1)

# Check if there were any male survivors
male_survivors = survivors.filter(col("Sex") == "male")
male_survivor_count = male_survivors.count()

# Check if there were any female survivors
female_survivors = survivors.filter(col("Sex") == "female")
female_survivor_count = female_survivors.count()

# Print the count of male and female survivors
print("---------------------------------------survived------------------------------")
print("Number of male survivors:", male_survivor_count)
print("Number of female survivors:", female_survivor_count)
print("---------------------------------------survived------------------------------")


# Stop the Spark session
spark.stop()

