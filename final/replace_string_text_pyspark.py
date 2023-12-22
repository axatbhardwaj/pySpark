from pyspark.sql import SparkSession
from pyspark.sql import Row
import os
import re

# Create a Spark session
spark = SparkSession.builder.appName("TextFilesToDF").getOrCreate()

# Specify the directory containing the text files
input_directory = "/content/sample_data/123"

# Get a list of all text files in the directory
text_files = [f for f in os.listdir(input_directory) if f.endswith(".txt")]

# Define email and credit card number formats
email_id_formats = [
    r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
]

credit_card_number_formats = [
    r"\b^4[0-9]{12}(?:[0-9]{3})?$\b",
    r"\b^5[1-5][0-9]{14}$\b",
    # ... (other credit card number formats)
]

# Combine the email and credit card patterns
combined_pattern = re.compile("|".join(email_id_formats + credit_card_number_formats))

# Define a function to read the content of a text file and apply replacements
def process_text_file(file_name):
    file_path = os.path.join(input_directory, file_name)
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    # Replace credit cards and email addresses with placeholders
    modified_content, count = re.subn(combined_pattern, 'was_a_credit_card_or_email', content)

    # Replace newline characters with a space
    modified_content = modified_content.replace('\n', ' ')

    return file_name, file_path, content, modified_content, count

# Create an RDD with the file information
files_rdd = spark.sparkContext.parallelize(text_files)
files_info_rdd = files_rdd.map(process_text_file)

# Create a DataFrame from the RDD
columns = ["filename", "filepath", "content", "modified_content", "count_of_changes"]
files_df = files_info_rdd.map(lambda x: Row(filename=x[0], filepath=x[1], content=x[2], modified_content=x[3], count_of_changes=x[4])).toDF(columns)

# Show the DataFrame
files_df.show(truncate=True)

# Optionally, you can save the DataFrame to a Parquet file or another format
# files_df.write.parquet("/path/to/save/df_with_changes.parquet")

# Stop the Spark session
spark.stop()
