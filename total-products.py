from datetime import datetime
from pyspark.context import SparkContext
import pyspark.sql.functions as f
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session
 
glue_db = "transactional-data-glue-database"
glue_table = "raw_transactional_data"
s3_write_path = "s3://processed-transactional-data/total-transactions/"

dt_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("Start time:", dt_start)
 
dynamic_frame_read = glue_context.create_dynamic_frame.from_catalog(database = glue_db, table_name = glue_table)
data_frame = dynamic_frame_read.toDF()
df_dropna = data_frame.dropna()
df_dropna = (df_dropna.withColumn('Order Date', f.from_unixtime(f.unix_timestamp('Order Date', 'MM/dd/yy HH:mm'))))
df_dropna = (df_dropna
                    .withColumn('hour', f.date_format(df_dropna['Order Date'], 'HH').cast(ptypes.IntegerType()))
                    .withColumn('minute', f.date_format(df_dropna['Order Date'], 'mm').cast(ptypes.IntegerType()))
                    .withColumn('weekday', f.date_format(df_dropna['Order Date'], 'EEEE'))
                    .withColumn('month', f.date_format(df_dropna['Order Date'], 'MMM')))
totaldf =(df_dropna.groupby('month_ordered').agg(f.count(f.col('Quantity Ordered')).alias('Quantity Ordered')))

dynamic_frame_write = DynamicFrame.fromDF(totaldf, glue_context, "dynamic_frame_write")
glue_context.write_dynamic_frame.from_options(frame = dynamic_frame_write,connection_type = "s3",
connection_options = {"path": s3_write_path},format = "csv")

dt_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("Start time:", dt_end)
