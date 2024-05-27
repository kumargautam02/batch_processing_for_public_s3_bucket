import re
import os
import s3fs
import logging
# from data_ingestion import ingest_data_from_s3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from general_functions import *
import matplotlib.pyplot as plt
# import matplotlib.pyplot as pyplt
import numpy as np
from scipy.interpolate import make_interp_spline
import datetime
from scipy.ndimage import gaussian_filter1d

from pyspark.sql.window import Window
from pyspark.sql.functions import *

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger('Data_Processing')

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from os.path import expanduser, join, abspath

sparkConf = SparkConf()
sparkConf.setMaster("spark://spark-master:4040")
sparkConf.setAppName("pyspark-4")
sparkConf.set("spark.executor.memory", "2g")
sparkConf.set("spark.driver.memory", "2g")
sparkConf.set("spark.executor.cores", "1")
sparkConf.set("spark.driver.cores", "1")
sparkConf.set("spark.dynamicAllocation.enabled", "false")
sparkConf.set("spark.shuffle.service.enabled", "false")
sparkConf.set("spark.sql.legacy.timeParserPolicy","LEGACY")


# def generating_publisher_id_with_maximum_queries(spark, destination_path):
#     try:

#         #data manipulation before graph generation
#         # spark = SparkSession.builder.master("local[*]").appName("click_logs").config("spark.sql.legacy.timeParserPolicy","LEGACY").getOrCreate()
#         # df = spark.read.parquet("C:/Users/Admin/Downloads/read_public_s3/clean/") 
#         df  = spark.read.parquet(destination_path)
        
#         print("data reading done",df.count())
#         df = df.withColumn("file_creation_date", date_format('file_creation_date', "yyyy-MM-dd"))
#         # display(df)
#         df.count()

#         # COMMAND ----------

#         df.printSchema()

#         # COMMAND ----------

#         # # publisher_id  = get_publisher_id_column_name(df)
#         window_spec = Window.partitionBy('publisher_id').orderBy("publisher_id")

#         # x = df.filter((df.file_creation_date).cast(StringType()).like('%2024-05-20%'))
#         x = df.withColumn("total_count_of_click", sum(col('total_clicks')).over(window_spec))
#         rank_spec = Window.partitionBy().orderBy(desc(col("total_count_of_click")))

#         x = x.withColumn("rank", dense_rank().over(rank_spec))

#         x = x.filter(x.rank<=5)
#         # display(x)
#         unique_publisher_id = list(set(x.select("publisher_id").rdd.flatMap(lambda x: x).collect()))
#         print(unique_publisher_id)

#         # # df = df.select(publisher_id, "count_of_click", "file_creation_date")
#         # display(x)

#         # COMMAND ----------

#         v = df.filter(df.publisher_id.isin(unique_publisher_id))
#         v.count()
#         v = v.groupBy("publisher_id", "file_creation_date").agg(sum(col("total_clicks")))
#         v = v.sort("publisher_id", "file_creation_date", ascending=True)
#         # display(v)
#         v.show()

#         # COMMAND ----------

#         v.printSchema()
#         return v
#     except Exception as e:
#         logger.info(f"Error has been encountered at apply_transformations {e}")






# def generatring_line_graph_for_top_5_publishers(v):
#     try:


#         pyplt.rcParams["figure.figsize"] = (50,15)
#         plt.rcParams["figure.autolayout"] = True
#         # fig, ax = plt.subplots(figsize=(5, 2.7), layout='constrained')
#         color_schema = ['r','y','g','k','c']
#         unique_publisher_id = list(set(v.select("publisher_id").rdd.flatMap(lambda x: x).collect()))
#         for i in range(len(unique_publisher_id)):
#             print(unique_publisher_id[i])
#             a = v.filter(v.publisher_id == unique_publisher_id[i])
#             x_axis = a.select("file_creation_date").rdd.flatMap(lambda x: x).collect()
#             y_axis = np.array(list(map(lambda x: int(x),a.select("sum(total_clicks)").rdd.flatMap(lambda x: x).collect())))//1000
#             print(y_axis)
#             # plt.ylim(min(y_axis), max(y_axis))
        
#             # plt.plot(x_axis, y_axis, f'o-{color_schema[i]}',label=unique_publisher_id[i], linewidth=10)
#             # Apply Gaussian filter for smoothing
#             y_smooth = gaussian_filter1d(y_axis, sigma=1)

#             # Plot original points
#             # plt.plot(dates, y, 'o', alpha=0.6, label='Original data')

#             # Plot smooth curve
#             plt.plot(x_axis, y_smooth,  f'x-{color_schema[i]}',marker='x',label=unique_publisher_id[i], linewidth=4)


#         plt.xlabel('Date', size = 50, labelpad=38)
#         plt.ylabel('Clicks (x 1000)', size = 50, labelpad= 38)
#         plt.title('QPS', size = 50, pad = 6)
#         plt.xticks(fontsize=42,rotation=45,ha='right')
#         plt.yticks(fontsize=42)
#         # plt.
#         specific_y_ticks = np.arange(0, 1200, 100)

#         plt.gca().set_yticks(specific_y_ticks)
#         plt.grid(visible = True,axis='y', which='both',color='k', linestyle='-', linewidth=0.6, in_layout=True)
#         plt.grid(visible = True,axis='x', which='both',color='k', linestyle='-', linewidth=0.6, in_layout=True)
#         # plt.yaxis.grid(True, which='minor')
#         # plt.grid()

#         # Just for appearance's sake
#         # ax.margins(0.05)
#         # ax.axis('tight')
#         # plt.tight_layout()
#         plt.legend(prop={'size':50})
#         plt.savefig('C:/Users/Admin/Downloads/read_public_s3/graph/line_graph.png', dpi=300, bbox_inches='tight')
#         plt.show()
#     except Exception as e:
#         logger.info(f"Error has been encountered at apply_transformations {e}")



def apply_transformations(spark,destination_path):
    try:
        print(destination_path)
        folder_path = []
        # actual_path = destination_path.replace("\\", "/")+'/Landing/click_log/2024/05/'
        actual_path = destination_path
        for outside in os.listdir(actual_path):
            for inside in os.listdir(f'{actual_path}/{outside}'):
                folder_path.append(f'{outside}/{inside}')


        for path in folder_path:
            print(path)

            df = spark.read.option("inferSchema", True).option("mode", "PERMISSIVE").json(f"{actual_path}{path}/")

            df = df.select('*', "ip_geo.*", "query.*").drop("query", "ip_geo")
            df = df.toDF(*get_unique_column_names(df.columns))

            df = df.drop(*get_duplicate_column_names(df))

            df = df.withColumn("real_filepath", input_file_name())

            df = df.withColumn("actual_file" , split(df.real_filepath, '/',limit=-1))
            df = df.withColumn("count_file", size(df.actual_file))
            df = df.withColumn("actual_file" , df.actual_file[col("count_file")-1]).drop("count_file")
            df = df.withColumn("file_creation_date", get_file_generation_date_udf(col("actual_file")))
  
            df = df.withColumn("file_creation_date", date_format(to_timestamp("file_creation_date", "yyyy-MM-dd HH-mm"), "yyyy-MM-dd HH:mm"))
            
            publisher_id  = get_publisher_id_column_name(df)
            

            df = df.na.fill("null")
            
            # print("this is the column structure", df.columns)
            df = df.withColumnRenamed(publisher_id, "publisher_id")
            df = df.select("publisher_id", "file_creation_date", "actual_file")
            df = df.withColumn("publisher_id", when(length(col("publisher_id")) > 6, regexp_extract(col("publisher_id"), "^(va-\d{3})|^(VA-\d{3})",0)).otherwise(col("publisher_id")))
            df = df.withColumn("path", lit(path))
            
            
            df = df.groupBy("publisher_id", "file_creation_date", "actual_file").agg(count("publisher_id").alias("total_clicks"))
            # # print("this is the window function count", df.count())
            df = df.withColumn("date", split(col("file_creation_date"), " ").getItem(0))
            df = df.withColumn("date", to_timestamp("date", "yyyy-MM-dd"))
            df.printSchema()

            df.write.partitionBy("date").mode("append").format("parquet").save(str(os.getcwd()).replace("\\", "/")+f'/clean1')
            logger.info(f"successfully saved data of {path} with partiton column date")
                # display(df)
                # if path == '20/07':
            # break
        return str(os.getcwd()).replace("\\", "/")+f'/clean1'
    except Exception as e:
        logger.info(f"Error has been encountered at apply_transformations {e}")


def generating_publisher_id_with_maximum_queries(spark, destination_path):
    """
    This function is used to transform the data to get the top-5 publisher_id, who are generating maximum queries.
    Parameters:
    spark[SparkSession object]: spark object.
    destination_path[python string]: this is the path of clean layer.

    returns: [pyspark dataframe] dataframe.
    """
    try:

        df  = spark.read.parquet(destination_path)
        
        print("data reading done",df.count())
        # df = df.withColumn("file_creation_date", date_format('file_creation_date', "yyyy-MM-dd"))
        # display(df)
        df.count()

        # COMMAND ----------

        df.printSchema()

        # COMMAND ----------

        # # publisher_id  = get_publisher_id_column_name(df)
        window_spec = Window.partitionBy('publisher_id').orderBy("publisher_id")

        # x = df.filter((df.file_creation_date).cast(StringType()).like('%2024-05-20%'))
        x = df.withColumn("total_count_of_click", sum(col('total_clicks')).over(window_spec))
        rank_spec = Window.partitionBy().orderBy(desc(col("total_count_of_click")))

        x = x.withColumn("rank", dense_rank().over(rank_spec))

        x = x.filter(x.rank<=5)
        # display(x)
        unique_publisher_id = list(set(x.select("publisher_id").rdd.flatMap(lambda x: x).collect()))
        print(unique_publisher_id)

        # # df = df.select(publisher_id, "count_of_click", "file_creation_date")
        # display(x)

        # COMMAND ----------

        v = df.filter(df.publisher_id.isin(unique_publisher_id))
        v.count()
        v = v.groupBy("publisher_id", "date").agg(sum(col("total_clicks")))
        v = v.sort("publisher_id", "date", ascending=True)
        # display(v)
        v.show()

        # COMMAND ----------

        v.printSchema()
        return v
    except Exception as e:
        logger.info(f"Error has been encountered at generating_publisher_id_with_maximum_queries {e}")


def generatring_line_graph_for_top_5_publishers(v):
    """
    This function is used to generated line-Graph for the top-5 publisher_id based on date.
    Parameters:
    v[pyspark dataframe]: dataframe
    output: saves a png file, inside graph folder.

    returns: None"""
    try:
        import pickle 
        # import matplotlib.pyplot as plt

        # create figure
        fig = plt.figure()
        # plt.plot([4,2,3,1,5])

        # # save whole figure 
        

        plt.rcParams["figure.figsize"] = (50,15)
        plt.rcParams["figure.autolayout"] = True
        # fig, ax = plt.subplots(figsize=(5, 2.7), layout='constrained')
        color_schema = ['r','y','g','k','c']
        unique_publisher_id = list(set(v.select("publisher_id").rdd.flatMap(lambda x: x).collect()))
        for i in range(len(unique_publisher_id)):
            print(unique_publisher_id[i])
            a = v.filter(v.publisher_id == unique_publisher_id[i])
            x_axis = a.select("date").rdd.flatMap(lambda x: x).collect()
            y_axis = np.array(list(map(lambda x: int(x),a.select("sum(total_clicks)").rdd.flatMap(lambda x: x).collect())))//1000
            print(y_axis)
            # plt.ylim(min(y_axis), max(y_axis))
        
            # plt.plot(x_axis, y_axis, f'o-{color_schema[i]}',label=unique_publisher_id[i], linewidth=10)
            # Apply Gaussian filter for smoothing
            y_smooth = gaussian_filter1d(y_axis, sigma=1)

            # Plot original points
            # plt.plot(dates, y, 'o', alpha=0.6, label='Original data')

            # Plot smooth curve
            plt.plot(x_axis, y_smooth,  f'x-{color_schema[i]}',label=unique_publisher_id[i], linewidth=4)


        plt.xlabel('Date', size = 50, labelpad=38)
        plt.ylabel('Clicks (x 1000)', size = 50, labelpad= 38)
        plt.title('QPS', size = 50, pad = 6)
        plt.xticks(fontsize=42,rotation=45,ha='right')
        plt.yticks(fontsize=42)
        # plt.
        specific_y_ticks = np.arange(0, 1200, 100)

        plt.gca().set_yticks(specific_y_ticks)
        plt.grid(visible = True,axis='y', which='both',color='k', linestyle='-', linewidth=0.6, in_layout=True)
        plt.grid(visible = True,axis='x', which='both',color='k', linestyle='-', linewidth=0.6, in_layout=True)
        # plt.yaxis.grid(True, which='minor')
        # plt.grid()

        # Just for appearance's sake
        # ax.margins(0.05)
        # ax.axis('tight')
        # plt.tight_layout()
        plt.legend(prop={'size':50})
        
        plt.show()
        plt.savefig('C:/Users/Admin/Downloads/backup/graph/output.pdf', dpi=10000, bbox_inches='tight')
        # pickle.dump(fig, open("figure.pickle", "wb"))
    except Exception as e:
        logger.info(f"Error has been encountered at generatring_line_graph_for_top_5_publishers {e}")



try:

    if __name__ == "__main__":
        logger.info('main.py Script started')

            
        import subprocess

        spark_submit_str= "spark-submit --master local[*] --deploy-mode client data_processing.py"
        process=subprocess.Popen(spark_submit_str,stdout=subprocess.PIPE,stderr=subprocess.PIPE, universal_newlines=True, shell=True)
        stdout,stderr = process.communicate()
        if process.returncode !=0:
            print(stderr)
            print(stdout)

        #public s3 path. 
        s3_path = 'datasci-assignment/click_log/2024/05/'

        #Create S3 object to read from public S3-bucket.
        s3 = s3fs.S3FileSystem(anon =  True)

        #getting currect working directory to save the files in landing location. 
        currect_working_directory = os.getcwd().replace("\\", "/")
        logger.info(f"ingestion of data started from s3-path {s3_path}")

        #Start INgesting data in Landing folder.
        destination_path  = ingest_data_from_s3(currect_working_directory, s3, s3_path)
        logger.info(f"ingestion of data Completed successfully at location {destination_path}")


        spark = SparkSession.builder.master("local[*]").appName("click_logs").config("spark.sql.legacy.timeParserPolicy","LEGACY").config(conf=sparkConf).getOrCreate()
        logger.info(f"SparkSession Created Successfully")
        
        logger.info(f"apply_transformations function started successfully reading data from location : {destination_path}")
        destination_path = apply_transformations(spark,destination_path)
        logger.info(f"apply_transformations function completed saved parquet at location: {destination_path}")
        df = generating_publisher_id_with_maximum_queries(spark, destination_path)
        df.show()
        # generatring_line_graph_for_top_5_publishers(df)
        

        
            
except Exception as e:
        logger.info(f"Error has been encountered at main.py {e}")