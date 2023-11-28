from pyspark import SparkConf
conf = SparkConf()
conf.set('spark.hadoop.fs.s3a.access.key','AKIA6GL5OZOXYIJNYQ4Y')
conf.set('spark.hadoop.fs.s3a.secret.key', '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')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.5')

