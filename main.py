# Databricks notebook source
!pip install -r /Workspace/Users/gautam.kumar410305@gmail.com/requirements.txt

# COMMAND ----------

pip install s3fs

# COMMAND ----------

import re
import os
# import s3fs
# from data_ingestion import ingest_data_from_s3
# from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col, udf,to_timestamp
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

def ingest_data_from_s3(current_working, s3, s3_path):
    """
    This function is used to Ingest data from Public S3 bucket site using s3fs library and store inside Landing Folder of Current working Directory. 
    Parameters:
    current_working: receive the current working directory to save data ex:- current_working_directory/Landing
    s3: S3 object for Public s3_bucket.
    s3_path: s3 bucket path from which we have to ingest data. 

    returns: None
    """

    # current_working = os.getcwd()
    # print(current_working)
    # s3 = s3fs.S3FileSystem(anon =  True)
    s3.get(f'{s3_path}', f'{current_working}/Landing1/',recursive=True, maxdepth=None)


def get_file_generation_date(column):
    pattern_for_date = "(2024-05-\d{2})"
    pattern_for_whole = "(2024-05-\d{2}-\d{2}-\d{2}-\d{2})"
    date_value = re.search(pattern_for_date, column).group(0)
    whole_value = re.search(pattern_for_whole, column).group(0)
    time_value = whole_value.replace(f'{date_value}-', "")

    return f"{date_value} {time_value}"



get_file_generation_date_udf  = udf(lambda column: get_file_generation_date(column),StringType())


def get_unique_column_names(column_names):
    # print(column_names)
    for i in range(len(column_names)):
        if column_names[i].strip() != "file_creation_date":
            column_names[i] = column_names[i] + f"_{i}" 

    return column_names


def get_duplicate_column_names(df):
    duplicate_columns = []
    original_columns = []
    for column in df.columns:
        # print(column)
        if column.rsplit("_", 1)[0] not in original_columns:
            original_columns.append(column.rsplit("_", 1)[0])
        else:
            duplicate_columns.append(column)
    return duplicate_columns


def get_publisher_id_column_name(df):
    # print("yes")
    for column in df.columns:
        # print(column)
        if column.rsplit("_", 1)[0] == 'publisher_id':
            # print(column)
            
            return column

# COMMAND ----------

s3_path = 'datasci-assignment/click_log/'
# # #Create S3 object to read from public S3-bucket.
# s3 = s3fs.S3FileSystem(anon =  True)

# # #Start INgesting data in Landing folder.
# ingest_data_from_s3("/Workspace/Users/gautam.kumar410305@gmail.com/", s3, s3_path)
# print(str(os.getcwd()).replace("\\", "/"))
# print(str(os.getcwd()).replace("\\", "/"))

from pyspark.sql.functions import *

df = spark.read.option("InferSchema", "True").option("mode", "PERMISSIVE").option("multiLine", True).json("file:/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/click_log/2024/05/10/00/")
df = df.select('*', "ip_geo.*", "query.*").drop("query", "ip_geo")
# df.write.format("parquet")
df = df.withColumn("real_filepath", input_file_name())

df = df.withColumn("actual_file" , split(df.real_filepath, '/',limit=-1))
df = df.withColumn("count_file", size(df.actual_file))
df = df.withColumn("actual_file" , df.actual_file[col("count_file")-1]).drop("count_file")
# df.withColum("filepath", F.regexp_extract("filepath", "State=(.+)\.snappy\.parquet", 1)
df.show(truncate=False)

# COMMAND ----------

for i in os.listdir("/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/click_log/2024/05/10/"):
    print(i)
    df = spark.read.option("inferSchema", True).option("mode", "PERMISSIVE").option("multiLine", True).json(f"file:/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/click_log/2024/05/10/{i}/")
    df.printSchema()
    df = df.select('*', "ip_geo.*", "query.*").drop("query", "ip_geo")
    # df.write.format("parquet")
    df = df.withColumn("real_filepath", input_file_name())

    df = df.withColumn("actual_file" , split(df.real_filepath, '/',limit=-1))
    df = df.withColumn("count_file", size(df.actual_file))
    df = df.withColumn("actual_file" , df.actual_file[col("count_file")-1]).drop("count_file")
    df = df.withColumn("file_creation_date", get_file_generation_date_udf(col("actual_file")))


    df = df.withColumn("file_creation_date", df["file_creation_date"].cast(DateType()))
    df.printSchema()
    display(df)
    df.write.option("mode","append").format("parquet").save("/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/clean/")
    # df.show()
    break

# COMMAND ----------


for i in os.listdir("/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/click_log/2024/05/10/"):
    print(i)
    df = spark.read.option("inferSchema", True).option("mode", "PERMISSIVE").option("multiLine", True).json(f"file:/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/click_log/")
    df.printSchema()
    df = df.select('*', "ip_geo.*", "query.*").drop("query", "ip_geo")
    # df.write.format("parquet")
    df = df.withColumn("real_filepath", input_file_name())

    df = df.withColumn("actual_file" , split(df.real_filepath, '/',limit=-1))
    df = df.withColumn("count_file", size(df.actual_file))
    df = df.withColumn("actual_file" , df.actual_file[col("count_file")-1]).drop("count_file")
    df = df.withColumn("file_creation_date", get_file_generation_date_udf(col("actual_file")))


    df = df.withColumn("file_creation_date", df["file_creation_date"].cast(DateType()))
    df.printSchema()
    display(df)
    df.write.option("mode","append").format("parquet").save("/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/clean/")
    # df.show()
    break

# COMMAND ----------

s3_path = 'datasci-assignment/click_log/2024/05/'
print(len(s3.find(s3_path, maxdepth=None)[1:]))

# COMMAND ----------

def get_unique_column_names(column_names):
    print(column_names)
    for i in range(len(column_names)):
        column_names[i] = column_names[i] + f"_{i}" 
    return column_names

# COMMAND ----------

# DBTITLE 1,This is to directly read data from S3

for i in s3.find(s3_path, maxdepth=None)[1:]:
    print(i)
    df = spark.read.option("inferSchema", True).option("mode", "PERMISSIVE").option("multiLine", True).json(f"s3://{i}")
    df.printSchema()
    df = df.select('*', "ip_geo.*", "query.*").drop("query", "ip_geo")
    # df.write.format("parquet")
    df = df.withColumn("real_filepath", input_file_name())

    df = df.withColumn("actual_file" , split(df.real_filepath, '/',limit=-1))
    df = df.withColumn("count_file", size(df.actual_file))
    df = df.withColumn("actual_file" , df.actual_file[col("count_file")-1]).drop("count_file")
    df = df.withColumn("file_creation_date", get_file_generation_date_udf(col("actual_file")))


    df = df.withColumn("file_creation_date", df["file_creation_date"].cast(DateType()))
    df.printSchema()
    # display(df)
    print(get_unique_column_names(df.columns))
    df = df.toDF(*get_unique_column_names(df.columns))
    df.printSchema()
    df.write.partitionBy("file_creation_date_29").mode("append").format("parquet").save("/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/clean")
    # df.show()
    # break

# COMMAND ----------

dbutils.fs.ls("/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/clean/file_creation_date_29=2024-05-10/")

# COMMAND ----------


# from pyspark.sql.functions import col, udf
# from pyspark.sql.types import *

# # # Converting function to UDF 
# # convertUDF = udf(lambda z: convertCase(z),StringType())
# def get_file_generation_date(column):
#     pattern = "(2024-05-\d{2})"
#     matched_value = re.search(pattern, column) 
#     return matched_value.group(0)

# get_file_generation_date_udf  = udf(lambda column: get_file_generation_date(column),StringType())

# COMMAND ----------

df = df.withColumn("file_creation_date", get_file_generation_date_udf(col("actual_file")))


df = df.withColumn("file_creation_date", df["file_creation_date"].cast(DateType()))
display(df)

# COMMAND ----------

# folder_path = []
# for outside in os.listdir("/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/click_log/2024/05/"):
#     for inside in os.listdir(f"/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/click_log/2024/05/{outside}/"):
#         folder_path.append(f'{outside}/{inside}')
# print(folder_path)

# COMMAND ----------

def create_table_meta_data(df):
    df.printSchema()
    if 'table_meta_data' not in os.listdir("/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/"):
        print(os.makedirs(f'/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/table_meta_data'))

    if len(os.listdir('/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/table_meta_data'))==0:
        # Create an empty dataframe with empty schema
        schema = StructType([StructField("column_list", ArrayType(StringType()))])
        # notice extra square brackets around each element of list 
        test_list = [[df.columns]]
        df = spark.createDataFrame(test_list,schema=schema)
        df = df.withColumn("column_list", explode(df.column_list)) 
        
        df.coalesce(1).write.mode("overwrite").format('json').save('file:/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/table_meta_data/')

        
def already_present_columns(df):


    already_present_columns = spark.read.option("inferSchema", True).option("mode", "PERMISSIVE").option("multiLine", True).json(f"file:/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/table_meta_data/").select("column_list").rdd.flatMap(lambda x: x).collect()
    first_set = set(df.columns)
    sec_set = set(already_present_columns)
    differences = (first_set - sec_set).union(sec_set - first_set)
    print('Differences between two lists: ')
    print(differences)
    print(first_set - sec_set)
    print(sec_set - first_set)


    return df



# COMMAND ----------

folder_path = []

for outside in os.listdir("/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/click_log/2024/05/"):
    for inside in os.listdir(f"/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/click_log/2024/05/{outside}/"):
        folder_path.append(f'{outside}/{inside}')
print(folder_path)
def apply_transformations():
    folder_path = []

    for outside in os.listdir("/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/click_log/2024/05/"):
        for inside in os.listdir(f"/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/click_log/2024/05/{outside}/"):
            folder_path.append(f'{outside}/{inside}')

    actual_path = "/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/click_log/2024/05/"
    for path in folder_path:
        print(path)
        # if path.__contains__('20/'):
        #     print(path)
        df = spark.read.option("inferSchema", True).option("mode", "PERMISSIVE").json(f"file:{actual_path}{path}/")
        # print("this is the count of schema while reading", df.count())
        # df.printSchema()
        df = df.select('*', "ip_geo.*", "query.*").drop("query", "ip_geo")
        df = df.toDF(*get_unique_column_names(df.columns))
        # print("this is the len of columsn before drop -", len(df.columns), path)
        df = df.drop(*get_duplicate_column_names(df))
        # print("this is the len of columsn before drop -", len(df.columns), path)
        # df.write.format("parquet")
        df = df.withColumn("real_filepath", input_file_name())
        # df.printSchema()
        # already_present_column = already_present_columns(df)
        # print(already_present_column)
        df = df.withColumn("actual_file" , split(df.real_filepath, '/',limit=-1))
        df = df.withColumn("count_file", size(df.actual_file))
        df = df.withColumn("actual_file" , df.actual_file[col("count_file")-1]).drop("count_file")
        df = df.withColumn("file_creation_date", get_file_generation_date_udf(col("actual_file")))
        df = df.withColumn("file_creation_date", date_format(to_timestamp("file_creation_date", "yyyy-MM-dd HH-mm"), "yyyy-MM-dd HH:mm"))
        publisher_id  = get_publisher_id_column_name(df)
        # print("this is the window function count", df.count())
        #creating window function to calculate the counts of each publisher_id
        # window_spec = Window.partitionBy(publisher_id).orderBy("file_creation_date")
        # df = df.withColumn("count_of_click", count(col(publisher_id)).over(window_spec))
        # df = df.select(publisher_id, "count_of_click", "file_creation_date")
        df = df.na.fill("null")
        # print("this is the column structure", df.columns)
        df = df.withColumnRenamed(publisher_id, "publisher_id")
        df = df.select("publisher_id", "file_creation_date", "actual_file")
        df = df.withColumn("path", lit(path))
        
        
        df = df.groupBy("publisher_id", "file_creation_date", "actual_file").agg(count("publisher_id").alias("total_clicks"))
        # # display(df)
        # # print("this is the window function count", df.count())

        df.write.mode("append").format("parquet").save("file:/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/clean/")
            # display(df)
            # if path == '20/07':
            #     break
    return df
apply_transformations()


# COMMAND ----------


df = spark.read.parquet("file:/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/clean/") 
df.count()

# COMMAND ----------

display(df)
df = df.withColumn("file_creation_date", date_format("yyyy-MM-dd HH-mm", "yyyy-MM-dd HH"))
display(df)

# COMMAND ----------

# publisher_id  = get_publisher_id_column_name(df)
window_spec = Window.partitionBy('publisher_id',).orderBy("publisher_id")

x = df.filter((df.file_creation_date).cast(StringType()).like('%2024-05-20%'))
x = x.withColumn("total_count_of_click", sum(col('total_clicks')).over(window_spec))
rank_spec = Window.partitionBy().orderBy(desc(col("total_count_of_click")))
# x = x.filter((x.file_creation_date).cast(StringType()) == '2024-05-22')
x = x.withColumn("rank", dense_rank().over(rank_spec))

x = x.filter(x.rank<=5)

# df = df.select(publisher_id, "count_of_click", "file_creation_date")
display(x)

# COMMAND ----------



# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.pyplot as pyplt
import numpy as np

pyplt.rcParams["figure.figsize"] = (300,200)
color_schema = ['red','yellow','green','purple','blue']
unique_publisher_id = list(set(x.select("publisher_id").rdd.flatMap(lambda x: x).collect()))
for i in range(len(unique_publisher_id)):
    print(unique_publisher_id[i])
    a = x.filter(x.publisher_id == unique_publisher_id[i])
    x_axis = a.select("file_creation_date").rdd.flatMap(lambda x: x).collect()
    y_axis = a.select("total_clicks").rdd.flatMap(lambda x: x).collect()

    plt.plot(x_axis, y_axis, color=color_schema[i])
    # plt.title("QPS based on each publisher")
    # plt.xlabel("Queries per second")
    # plt.ylabel("publisher_id")
    # # plt.set_size_inches(18, 10, forward=True)
plt.show()



# COMMAND ----------



# COMMAND ----------

publisher_id  = get_publisher_id_column_name(a)
b = a.groupBy(publisher_id).

# window_spec = Window.partitionBy(publisher_id).orderBy("file_creation_date")
# b = a.withColumn("count_of_click", count(col(publisher_id)).over(window_spec))
# b = a.withColumn("count_of_clic1", count("*").over(window_spec))
# b = b.select(publisher_id, "count_of_click", "file_creation_date")


# COMMAND ----------

display(a)

# COMMAND ----------

def get_publisher_id_column_name(df):
    print("yes")
    for column in df.columns:
        print(column)
        if column.rsplit("_", 1)[0] == 'publisher_id':
            print(column)
            
            return column

get_publisher_id_column_name(a)

# COMMAND ----------

b = a.groupBy("publisher_id_8").agg(count(col("ip_5")))
b = b.na.fill("null")
display(b)


# COMMAND ----------

print("actual_file_32".rsplit("_", 1)[0])

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.pyplot as pyplt

pyplt.rcParams["figure.figsize"] = (30,5)

x_axis = b.select("publisher_id_8").rdd.flatMap(lambda x: x).collect()
y_axis = b.select("count(ip_5)").rdd.flatMap(lambda x: x).collect()

plt.plot(x_axis, y_axis)
plt.title("QPS based on each publisher")
plt.xlabel("Queries per second")
plt.ylabel("publisher_id")
# plt.set_size_inches(18, 10, forward=True)
plt.show()



# COMMAND ----------

column = "perf_click_data-2-2024-05-20-00-12-55-82b51e8a-cb2b-45aa-aa2c-20674e9fee09.gz"
pattern_for_date = "(2024-05-\d{2})"
pattern_for_whole = "(2024-05-\d{2}-\d{2}-\d{2}-\d{2})"
date_value = re.search(pattern_for_date, column).group(0)
whole_value = re.search(pattern_for_whole, column).group(0)
time_value = whole_value.replace(f'{date_value}-', "")

return f"{date_value} {time_value}"



# COMMAND ----------

df = spark.read.option("inferSchema", True).option("mode", "PERMISSIVE").json(f"file:/Workspace/Users/gautam.kumar410305@gmail.com/Landing1/click_log/2024/05/20/00/")
df = df.select('*', "ip_geo.*", "query.*").drop("query", "ip_geo")
df.printSchema()
df = df.toDF(*get_unique_column_names(df.columns))
# print("this is the len of columsn before drop -", len(df.columns), path)
df = df.drop(*get_duplicate_column_names(df))
# print("this is the len of columsn before drop -", len(df.columns), path)
# df.write.format("parquet")
df = df.withColumn("real_filepath", input_file_name())
# df.printSchema()
# already_present_column = already_present_columns(df)
# print(already_present_column)
df = df.withColumn("actual_file" , split(df.real_filepath, '/',limit=-1))
df = df.withColumn("count_file", size(df.actual_file))
df = df.withColumn("actual_file" , df.actual_file[col("count_file")-1]).drop("count_file")
df = df.withColumn("file_creation_date", get_file_generation_date_udf(col("actual_file")))
df = df.withColumn("file_creation_date", date_format(to_timestamp("file_creation_date", "yyyy-MM-dd HH-mm"), "yyyy-MM-dd HH:mm"))
publisher_id  = get_publisher_id_column_name(df)
print("this is the count of schema while reading", df.count())
display(df.count())

# COMMAND ----------

df = df.select(publisher_id, "file_creation_date", "actual_file")
x = df.groupBy(publisher_id, "file_creation_date", "actual_file").agg(count(publisher_id).alias("total_clicks"))
display(x)

# COMMAND ----------



# COMMAND ----------

display(df)

# COMMAND ----------

v = df.groupBy(publisher_id, "file_creation_date").agg(count('*'))

# COMMAND ----------

display(v)

# COMMAND ----------

window_spec = Window.partitionBy(publisher_id).orderBy("file_creation_date")
b = df.withColumn("count_of_click", count(col(publisher_id)).over(window_spec))
# b = a.withColumn("count_of_clic1", count("*").over(window_spec))
b = b.select("publisher_id_9", "count_of_click", "file_creation_date")
display(b)

# COMMAND ----------


