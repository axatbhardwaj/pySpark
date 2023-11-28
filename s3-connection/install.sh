# Install OpenJDK 8
# sudo pacman -S jdk8-openjdk

# Download and extract Spark
wget -q https://dlcdn.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz
tar xf spark-3.4.1-bin-hadoop3.tgz

# Install findspark
pip install findspark

# Set environment variables
export JAVA_HOME="/usr/lib/jvm/java-8-openjdk"
export SPARK_HOME="/home/axat/work/pySpark/pyspark-s3/spark-3.4.1-bin-hadoop3"
export PATH="$PATH:$SPARK_HOME/bin"

# Download Hadoop and AWS JARs
wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.5/hadoop-aws-3.3.5.jar
wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.595/aws-java-sdk-bundle-1.12.595.jar

# Move JARs to the Spark jars directory
mv aws-java-sdk-bundle-1.12.595.jar $SPARK_HOME/jars/
mv hadoop-aws-3.3.5.jar $SPARK_HOME/jars/

# Set AWS credentials
export AWS_ACCESS_KEY_ID="YOUR_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="YOUR_SECRET_ACCESS_KEY"