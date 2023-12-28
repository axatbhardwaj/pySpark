from pyspark import SparkContext,SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark
spark = SparkSession.builder.appName("S3BucketExample").getOrCreate()
sc = spark.sparkContext

# Replace 'YOUR_S3_LINK' with your actual S3 link
s3_link = "https://deepak-test-infrablok.s3.amazonaws.com/tested.csv"

# Add the S3 file to SparkContext
sc.addFile(s3_link)

# Access the file using the SparkFiles API
file_path = "file://" + SparkFiles.get("tested.csv")

# Read data from the file
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Example: Display the DataFrame
df.show()

# Your Spark processing logic here...

# Stop Spark
spark.stop()
