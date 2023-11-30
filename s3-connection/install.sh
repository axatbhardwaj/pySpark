# Install OpenJDK 8
# sudo pacman -S jdk8-openjdk

# Download and extract Spark
wget  https://dlcdn.apache.org/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz
tar xf spark-3.4.1-bin-hadoop3.tgz
rm spark-3.4.1-bin-hadoop3.tgz

# Install findspark
pip install findspark

# Download Hadoop and AWS JARs
wget  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
wget  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.595/aws-java-sdk-bundle-1.12.595.jar

# Move JARs to the Spark jars directory
mv aws-java-sdk-bundle-1.12.595.jar `pwd`/spark-3.4.1-bin-hadoop3/jars
mv hadoop-aws-3.3.4.jar `pwd`/spark-3.4.1-bin-hadoop3/jars
