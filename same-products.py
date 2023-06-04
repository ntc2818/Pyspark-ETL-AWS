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
 
glue_db = "dbname"
glue_table = "tablename"
s3_write_path = "s3outputpath"

dt_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("Start time:", dt_start)
 
dynamic_frame_read = glue_context.create_dynamic_frame.from_catalog(database = glue_db, table_name = glue_table)
data_frame = dynamic_frame_read.toDF()

df_dropna = data_frame.dropna()
multipledf = (df_dropna.groupby('Order ID').count().where(f.col('count') > 1).sort('count', ascending=False)
             .withColumnRenamed('count', 'total_item_in_order'))
multipledf.count()
df_dropna_tmp = df_dropna
multipledf2 = multipledf.join(df_dropna_tmp, ['Order ID'], 'leftouter')
multipledf2.count()
multipledf3 =(multipledf2.groupby('Order ID').agg(f.concat_ws(", ", f.collect_list(f.col('Product'))).alias('items_order'),
             *[f.first(cl).alias(cl) for cl in multipledf2.columns if not cl == 'Order ID']))


dynamic_frame_write = DynamicFrame.fromDF(data_frame, glue_context, "dynamic_frame_write")
 
glue_context.write_dynamic_frame.from_options(frame = dynamic_frame_write,connection_type = "s3",
connection_options = {"path": s3_write_path},format = "csv")

dt_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("Start time:", dt_end)
