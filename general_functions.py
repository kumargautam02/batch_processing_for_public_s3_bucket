
import re
import logging
from pyspark.sql.functions import *
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger('Data_Processing')
logger.info('main.py Script started')

def ingest_data_from_s3(current_working, s3, s3_path):
    try:
        """
        This function is used to Ingest data from Public S3 bucket site using s3fs library and store inside Landing Folder of Current working Directory. 
        Parameters:
        current_working: receive the current working directory to save data ex:- current_working_directory/Landing
        s3: S3 object for Public s3_bucket.
        s3_path: s3 bucket path from which we have to ingest data. 

        returns: None
        """

        # current_working = os.getcwd()
        # s3.get(f'{s3_path}', f'{current_working}/Landing1/',recursive=True, maxdepth=None)
        return f'{current_working}/Landing1/'
    except Exception as e:
        logger.info(f"Error has been encountered at ingest_data_from_s3 {e}")


def get_file_generation_date(column):
    """
    This function is used to extract the date_column from file_name.
    Parameters:
    column[pyspark dataframe column]: column with the file_name.

    returns: the date of file generation.
    """
    try:
        pattern_for_date = "(2024-05-\d{2})"
        pattern_for_whole = "(2024-05-\d{2}-\d{2}-\d{2}-\d{2})"
        date_value = re.search(pattern_for_date, column).group(0)
        whole_value = re.search(pattern_for_whole, column).group(0)
        time_value = whole_value.replace(f'{date_value}-', "")

        return f"{date_value} {time_value}"
    except Exception as e:
        logger.info(f"Error has been encountered at get_file_generation_date {e}")



get_file_generation_date_udf  = udf(lambda column: get_file_generation_date(column),StringType())


def get_unique_column_names(column_names):
    """
    This function is used to give unique names to spark columns based on index of the columns.
    Parameters:
    column[python list]: Python list with the column_names

    returns: [python list with unique names]
    """
    try:
        # print(column_names)
        for i in range(len(column_names)):
            if column_names[i].strip() != "file_creation_date":
                column_names[i] = column_names[i] + f"_{i}" 

        return column_names
    except Exception as e:
        logger.info(f"Error has been encountered at get_unique_column_names {e}")
    

def get_duplicate_column_names(df):
    """
    This function is used to get the duplciate columns from the dataframe.
    Parameters:
    df[pyspark dataframe column]: pyspark dataframe

    returns: [Python list] list of duplicate column names present in the dataframe.
    """
    try:
        duplicate_columns = []
        original_columns = []
        for column in df.columns:
            # print(column)
            if column.rsplit("_", 1)[0] not in original_columns:
                original_columns.append(column.rsplit("_", 1)[0])
            else:
                duplicate_columns.append(column)
        return duplicate_columns
    except Exception as e:
        logger.info(f"Error has been encountered at get_duplicate_column_names {e}")


def get_publisher_id_column_name(df):
    """
    This function is used to get the publisher_id column name.
    Parameters:
    column[pyspark dataframe column]: dataframe.

    returns: [python string] column name of publisher_id
    """
    try:
        # print("yes")
        for column in df.columns:
            # print(column)
            if column.rsplit("_", 1)[0] == 'publisher_id':
                # print(column)
                
                return column
    except Exception as e:
        logger.info(f"Error has been encountered at get_publisher_id_column_name {e}")





# def generating_publisher_id_with_maximum_queries(spark, destination_path):
#     """
#     This function is used to transform the data to get the top-5 publisher_id, who are generating maximum queries.
#     Parameters:
#     spark[SparkSession object]: spark object.
#     destination_path[python string]: this is the path of clean layer.

#     returns: [pyspark dataframe] dataframe.
#     """
#     try:

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
#         logger.info(f"Error has been encountered at generating_publisher_id_with_maximum_queries {e}")






# def generatring_line_graph_for_top_5_publishers(v,plt):
#     """
#     This function is used to generated line-Graph for the top-5 publisher_id based on date.
#     Parameters:
#     v[pyspark dataframe]: dataframe
#     output: saves a png file, inside graph folder.

#     returns: None"""
#     try:

#         plt.rcParams["figure.figsize"] = (50,15)
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
#         logger.info(f"Error has been encountered at generatring_line_graph_for_top_5_publishers {e}")

