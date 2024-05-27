import re
import os
import logging
import s3fs
from data_ingestion import ingest_data_from_s3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *





spark = SparkSession.builder.master("local[*]").appName("click_logs").getOrCreate()

# s3_path = 'datasci-assignment/'
# #Create S3 object to read from public S3-bucket.
# s3 = s3fs.S3FileSystem(anon =  True)

# #Start INgesting data in Landing folder.
# ingest_data_from_s3(os.getcwd(), s3, s3_path)
print(str(os.getcwd()).replace("\\", "/"))



df = spark.read.option("InferSchema", "True").option("mode", "PERMISSIVE").option("multiLine", True).json("C:/Users/Admin/Downloads/read_public_s3/Landing/postback-log/2024/05/10/00/performance_postback_log-1-2024-05-10-00-00-33-efc64b50-edbc-4a15-97ae-0aaefa93ce79")
# df = df.select('*', "ip_geo.*", "query.*").drop("query", "ip_geo")
# # df.write.format("parquet")
# df = df.withColumn("real_filepath", input_file_name())

# df = df.withColumn("actual_file" , split(df.real_filepath, '/',limit=-1))
# df = df.withColumn("count_file", size(df.actual_file))
# df = df.withColumn("actual_file" , df.actual_file[col("count_file")-1]).drop("count_file")
# # df.withColum("filepath", F.regexp_extract("filepath", "State=(.+)\.snappy\.parquet", 1)

df.show()