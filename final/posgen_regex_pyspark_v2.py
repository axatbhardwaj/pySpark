"""V2 has imporved speeds by a huge margin on pyspark v1 used to take more than 30 mins on pyspark for 3mb txt file but 
v2 takes less than a minute on pyspark for 3mb txt file 
v3 will be about increasing parallelisation"""

from pyspark.sql import SparkSession
from pyspark.sql import Row
import os
import re
import json

# Create a Spark session
spark = SparkSession.builder.appName("TextFilesToDF").getOrCreate()

# Specify the directory containing the text files
input_directory = "/content/sample_data"

# Get a list of all text files in the directory
text_files = [f for f in os.listdir(input_directory) if f.endswith(".txt")]

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

def process_text_file(file_name):
    file_path = os.path.join(input_directory, file_name)
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
        changes_count=0

    changes_info = []

    for entity, regex_patterns in compiled_data_formats.items():
        for pattern in regex_patterns:
            for match in pattern.finditer(content):
                changes_info.append({
                    'key': entity,
                    'start': match.start(),
                    'end': match.end(),
                    'value': match.group(),
                    'n_window_text': get_n_window_text(content, match.start(), match.end())
                })
                changes_count=changes_count+1


    # return file_name, file_path, content, changes_info, len(changes_info)

    return file_name, file_path, content, changes_info, changes_count




# Create an RDD with the file information
files_rdd = spark.sparkContext.parallelize(text_files)
files_info_rdd = files_rdd.map(process_text_file)


columns = ["filename", "filepath", "content", "changes_info", "changes_count"]
files_df = files_info_rdd.map(lambda x: Row(filename=x[0], filepath=x[1], content=x[2], changes_info=x[3], changes_count=x[4])).toDF(columns)


# Extract the changes_info column as a list of strings
changes_info_list = files_df.select("changes_info").rdd.flatMap(lambda x: x).collect()
# print(changes_info_list)

# Save changes_info for each file to a separate text file
for row in files_info_rdd.collect():
    filename = row[0]
    changes_info = row[3]

    # Create a separate text file for each file
    output_file_path = f"/content/output/{filename}_changes_info.txt"

    with open(output_file_path, "w", encoding="utf-8") as file:
        for item in changes_info:
            file.write(json.dumps(item) + "\n")

# Stop the Spark session
spark.stop()
