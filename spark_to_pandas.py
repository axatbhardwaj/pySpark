from pyspark.sql import SparkSession , Row 
from pyspark.sql.functions import input_file_name, col, lit, udf , broadcast
from pyspark.sql.types import StringType, StructType, StructField, BinaryType
import os
import pandas as pd
import re

# Define all regex patterns
account_number_formats = [r'\b\d{6,17}\b']
races_list = [  
'Caucasian','African','Asian','Hispanic/Latino','Native American','Middle Eastern','Pacific Islander'
]
credit_card_number_formats = [
    r"\b^4[0-9]{12}(?:[0-9]{3})?$\b",
    r"\b^5[1-5][0-9]{14}$\b",
    r"\b^3[47][0-9]{13}$\b",
    r"\b^(6011|65|64[4-9])[0-9]{13}$\b",
    r"\b^3(?:0[0-5]|[68][0-9])[0-9]{11}$\b",
    r"\b^(?:2131|1800|35\d{3})\d{11}$\b",
    r"\b^(62|88)\d{14,17}$\b",
    r"\b(5[1-5][0-9]{2}[0-9]{4}[0-9]{4})|4[0-9]{3}[0-9]{4}[0-9]{4}[0-9]{4}|4[0-9]{3}[0-9]{4}[0-9]{4}[0-9]{1}|3[4,7][0-9]{2}[0-9]{6}[0-9]{5}|((6011)[0-9]{4}[0-9]{4}[0-9]{4})|((65)[0-9]{2}[0-9]{4}[0-9]{4}[0-9]{4})|(3[0478][0-9]{2}[0-9]{4}[0-9]{4}[0-9]{2,4})\b"
]

credit_card_cvv_number_formats = [r"(?<![-.@$&()\/\d])\b\d{3,4}(?![-.@$&()\/])\b"]
credit_card_expiry_date_formats = [r"(?<![-.@$&()\/\d])\b\d{2}([-.@$&()\/])\d{2}(?:\d{2})?(?![-.@$&()\/])\b|(?<![-.@$&()\/\d])\b\d{2}(?:\d{2})?([-.@$&()\/])\d{2}(?![-.@$&()\/])\b|\b(?:(?i)January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s\d{2}(?:\d{2})?\b|\b\d{2}(?:\d{2})?\s(?:(?i)January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b"]

credit_card_pin_number_formats = [r'(?<![-.@$&()\/\d])\b\d{4}(?:\d{2})?(?![-.@$&()\/])\b']

date_of_birth_formats = [r"\b(?:\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\b(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s(?:\d{1,2}(?:st|nd|rd|th)?,?\s)?\d{2,4})\b",
                         r"\b\d{1,2}(st|nd|rd|th)(|\W)[A-Za-z]{3,10}(|\W)\d{2,4}\b"]

driving_license_number_formats = [
    r"\b\d{7,12}\b",
    r"\b[a-zA-Z]\d{6,14}\b",
    r"\b[a-zA-Z]{1,2}\d{5,6}\b",
    r"\b\d{2}[a-zA-Z]{3}\d{5}\b",
    r"\b[a-zA-Z]{2}\d{6}[a-zA-Z]{1}\b",
    r"\b\d{3}[a-zA-Z]{1}\d{9}\b",
    r"\b\d{8}[a-zA-Z]{1,2}\b",
    r"\b\d{7}[a-zA-Z]{1}\b",
    r"\b[A-Za-z]{1}[0-9]{7}\b"
]

email_id_formats = [
    r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
]

phone_and_fax_number_formats = [
    r"\b(123)\W(\W)?\d{3}\W\d{4}\b",  # (123) 456-7890
    r"\b[+]\d{1}(\W)?\d{3}(\W)?\d{3}(\W)?\d{4}\b",  # +1 123-456-7890
    r'\b(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})\b',
    r"\b((?:(?:\+?1[-.\s])?\(?\d{3}\)?[-.\s])?\d{3}[-.\s]\d{4}(?:\s(?:x^|#^|[eE]xt[.]?^|[eE]xtension){1} ?\d{1,7})?)\b"
]

ipv4_formats = [
    r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"
]

ipv6_formats = [
    r"(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))"
]

medical_record_number_formats = [
    r"\b[A-Za-z]{1,5}\d{4,10}\b"
]

passport_number_formats = [
    r"\b[a-zA-Z]\d{8,9}\b",
    r"\b\d{8,9}\b"
]

provider_id_formats = [
    r"\b\d{10}\b"
]

religions_list = [
    "Christianity","Hinduism","Hindu","Buddhism","Judaism","Sikhism","Jainism","Bahá'í Faith","Islam","Taoism","Shintoism","Atheism","Agnosticism"
    ]

social_security_number_formats = [
    r"[0-9]{3}-[0-9]{2}-[0-9]{4}"
]

vehicle_license_plate_number_formats = [
    r"\b\d{6}\b",
    r"\b\d{1}[a-zA-Z]{3}\d{3}\b",
    r"\b[a-zA-Z]{2,3}\W?\d{4,5}\b"
]

vehicle_vin_number_formats = [
    r"\b[a-zA-z0-9]{17}\b"
]

web_url_formats = [
    r"(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})"
]

def get_n_window_text(text, start, end, n=100):
    return text[max(0, start-n):min(end+n, len(text))]

data_formats = {
    "account_number": account_number_formats,
    "credit_card_number": credit_card_number_formats,
    "credit_card_cvv_number": credit_card_cvv_number_formats,
    "credit_card_pin_number": credit_card_pin_number_formats,
    "credit_card_expiry_date": credit_card_expiry_date_formats,
    "date_of_birth": date_of_birth_formats,
    "email_id": email_id_formats,
    "phone_number": phone_and_fax_number_formats,
    "fax_number": phone_and_fax_number_formats,
    # "phone_and_fax_number": phone_and_fax_number_formats,
    "ipv4": ipv4_formats,
    "ipv6": ipv6_formats,
    "medical_record_number": medical_record_number_formats,
    "passport_number": passport_number_formats,
    "provider_id": provider_id_formats,
    "social_security_number": social_security_number_formats,
    "vehicle_license_plate_number": vehicle_license_plate_number_formats,
    "vehicle_vin_number": vehicle_vin_number_formats,
    "driving_license_number": driving_license_number_formats,
    "url": web_url_formats,
    "race": races_list,
    "religion": religions_list
}
# Compile the patterns for efficiency
compiled_data_formats = {key: [re.compile(pattern, re.IGNORECASE) for pattern in patterns]
                         for key, patterns in data_formats.items()}


#process text binary content

def process_text_binary_content(row):
    file_name = row['filename']
    file_path = row['filepath']
    binary_content = row['binary_content']

    # Convert binary_content to readable text
    content = binary_content.decode('utf-8')

    positions_info = None  # Leave positions_info empty for now
    position_count = 0
    position_info = []

    for entity, regex_patterns in compiled_data_formats.items():
        for pattern in regex_patterns:
            for match in pattern.finditer(content):
                position_info.append({
                    'key': entity,
                    'start': match.start(),
                    'end': match.end(),
                    'value': match.group(),
                    'n_window_text': get_n_window_text(content, match.start(), match.end())
                })
                position_count += 1

    # Create a new Row with updated content
    updated_row = Row(
        filename=row['filename'],
        filepath=row['filepath'],
        binary_content=row['binary_content'],
        positions_info=position_info,
        position_count=position_count
    )

    return updated_row


# Initialize a Spark session
spark_local = SparkSession.builder \
    .appName("FileReader") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()


# Set the input directory path
input_directory = '/home/ubuntu/50x'

# Define the schema for the DataFrame
schema = StructType([
    StructField("filename", StringType(), True),
    StructField("filepath", StringType(), True),
    StructField("binary_content", BinaryType(), True),
    StructField("file_extension", StringType(), True)
])

# UDF to extract filename from filepath and replace %20 with space
extract_filename_udf = udf(lambda filepath: os.path.basename(filepath).replace("%20", " "), StringType())

# Read DOCX files
docx_files_df = (
    spark_local.read.format("binaryFile")
    .option("pathGlobFilter", "*.docx")
    .load(input_directory)
    .select(
        input_file_name().alias("filepath"),
        "content"
    )
    .withColumn("filename", extract_filename_udf(col("filepath")))
    .withColumn("binary_content", col("content"))
    .withColumn("file_extension", lit("docx"))
    .select("filename", "filepath", "binary_content", "file_extension")
)

# Read TXT files
text_files_df = (
    spark_local.read.format("binaryFile")
    .option("pathGlobFilter", "*.txt")
    .load(input_directory)
    .select(
        input_file_name().alias("filepath"),
        "content"
    )
    .withColumn("filename", extract_filename_udf(col("filepath")))
    .withColumn("binary_content", col("content"))
    .withColumn("file_extension", lit("txt"))
    .select("filename", "filepath", "binary_content", "file_extension")
)

# Read PDF files
pdf_files_df = (
    spark_local.read.format("binaryFile")
    .option("pathGlobFilter", "*.pdf")
    .load(input_directory)
    .select(
        input_file_name().alias("filepath"),
        "content"
    )
    .withColumn("filename", extract_filename_udf(col("filepath")))
    .withColumn("binary_content", col("content"))
    .withColumn("file_extension", lit("pdf"))
    .select("filename", "filepath", "binary_content", "file_extension")
)

# Union the DataFrames


# Display the resulting DataFrame
# all_files_df.show(truncate=True)

text_files_df.show(truncate=True)

# Convert PySpark DataFrame to Pandas DataFrame
pandas_text_df = text_files_df.toPandas()
pandas_pdf_df=pdf_files_df.toPandas()
pandas_docx_df=docx_files_df.toPandas()

# Stop the local Spark session (SparkContext1)
spark_local.stop()

# pandas_text_df.


# Create the Spark session on the cluster (SparkContext2)
spark_cluster = SparkSession.builder \
    .appName("spark_2_PD_DF_50") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.master","spark://ip-10-0-239-11.ec2.internal:7077") \
    .getOrCreate()



# Convert Pandas DataFrame to PySpark DataFrame in SparkContext2
text_files_df_cluster = spark_cluster.createDataFrame(pandas_text_df)
pdf_files_df_cluster = spark_cluster.createDataFrame(pandas_pdf_df)
docx_files_df_cluster= spark_cluster.createDataFrame(pandas_docx_df)
# Show PySpark DataFrame in SparkContext2

# Use broadcast on the DataFrame if necessary (only if you have a small DataFrame to broadcast)
text_files_df_cluster = broadcast(text_files_df_cluster)

processed_text_files_rdd = text_files_df_cluster.rdd.map(process_text_binary_content)
processed_text_files_df = processed_text_files_rdd.toDF()

processed_text_files_df.show(truncate=True)

# Stop the Spark session on the cluster (SparkContext2)
spark_cluster.stop()

