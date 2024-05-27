# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.pyplot as pyplt
import numpy as np
from scipy.interpolate import make_interp_spline
import datetime
from scipy.ndimage import gaussian_filter1d
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

#data manipulation before graph generation
spark = SparkSession.builder.master("local[*]").appName("click_logs").config("spark.sql.legacy.timeParserPolicy","LEGACY").getOrCreate()
df = spark.read.parquet("C:/Users/Admin/Downloads/read_public_s3/clean/") 
df.count()
df = df.withColumn("file_creation_date", date_format('file_creation_date', "yyyy-MM-dd"))
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
v = v.groupBy("publisher_id", "file_creation_date").agg(sum(col("total_clicks")))
v = v.sort("publisher_id", "file_creation_date", ascending=True)
# display(v)
v.show()

# COMMAND ----------

v.printSchema()









pyplt.rcParams["figure.figsize"] = (50,15)
plt.rcParams["figure.autolayout"] = True
# fig, ax = plt.subplots(figsize=(5, 2.7), layout='constrained')
color_schema = ['r','y','g','k','c']
unique_publisher_id = list(set(v.select("publisher_id").rdd.flatMap(lambda x: x).collect()))
for i in range(len(unique_publisher_id)):
    print(unique_publisher_id[i])
    a = v.filter(v.publisher_id == unique_publisher_id[i])
    x_axis = a.select("file_creation_date").rdd.flatMap(lambda x: x).collect()
    y_axis = np.array(list(map(lambda x: int(x),a.select("sum(total_clicks)").rdd.flatMap(lambda x: x).collect())))//1000
    print(y_axis)
    # plt.ylim(min(y_axis), max(y_axis))
  
    # plt.plot(x_axis, y_axis, f'o-{color_schema[i]}',label=unique_publisher_id[i], linewidth=10)
    # Apply Gaussian filter for smoothing
    y_smooth = gaussian_filter1d(y_axis, sigma=1)

    # Plot original points
    # plt.plot(dates, y, 'o', alpha=0.6, label='Original data')

    # Plot smooth curve
    plt.plot(x_axis, y_smooth,  f'x-{color_schema[i]}',marker='x',label=unique_publisher_id[i], linewidth=4)


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
plt.savefig('C:/Users/Admin/Downloads/read_public_s3/graph/line_graph.png', dpi=300, bbox_inches='tight')

# saprk.await()