# pyspark --packages org.apache.hadoop:hadoop-aws:3.3.2

from pyspark import SparkConf
from pyspark.sql import SparkSession
 
conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')
 
spark = SparkSession.builder.getOrCreate()