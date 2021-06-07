# ensure output s3 temp directory given during job creation
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as F
## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "iot_raw_db", table_name = "temperature", transformation_ctx = "datasource0")


#applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("temp", "int", "max_temp", "double"), ("timestamp", "long", "min_temp", "double"), ("device_id", "string", "device_id", "string"), ("type", "string", "agg_time", "string")], transformation_ctx = "applymapping1")
#selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["agg_time", "device_id", "min_temp", "max_temp"], transformation_ctx = "selectfields2")
#resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "iot_aggregatedb", table_name = "dev_iot_aggregated_temp", transformation_ctx = "resolvechoice3")
#resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")


rawDF = datasource0.toDF()
rawDF.printSchema()

processed = rawDF\
            .withColumn("agg_time",  F.from_unixtime(F.col("timestamp") / 1000, 'yyyy-MM-dd HH'))\
            .groupBy("device_id", "agg_time")\
            .agg(F.min(F.col("temp")).alias("min_temp"),\
            F.max(F.col("temp")).alias("max_temp"))
            
processed.show(5)

dynamic_output_frame = DynamicFrame.fromDF(processed, glueContext, "dynamic_output_frame")

#resolvechoice4 = ResolveChoice.apply(frame = dynamic_output_frame, choice = "make_cols", transformation_ctx = "resolvechoice4")


datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = dynamic_output_frame, database = "iot_aggregatedb", table_name = "dev_iot_aggregated_temp", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink5")

#datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "iot_aggregatedb", table_name = "dev_iot_aggregated_temp", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink5")
print("Done writing result 2")

job.commit()
