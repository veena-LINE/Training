import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

SqlQuery0 = '''
select InvoiceDate,
       InvoiceNo,
       CustomerID,
       Description,
       Country,
       StockCode,
       sum(Quantity*UnitPrice) as Turnover
  from myDataSource
 group by 1, 2, 3, 4, 5, 6
'''

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [connection_type = "s3", format = "parquet", connection_options = {"paths": ["s3://bond-s3-forspark/invoice-application/"], "recurse":True}, transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_options(connection_type = "s3", format = "parquet", connection_options = {"paths": ["s3://bond-s3-forspark/invoice-application/"], "recurse":True}, transformation_ctx = "DataSource0")
## @type: SqlCode
## @args: [sqlAliases = {"myDataSource": DataSource0}, sqlName = SqlQuery0, transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [dfc = DataSource0]
Transform0 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource": DataSource0}, transformation_ctx = "Transform0")
## @type: DataSink
## @args: [connection_type = "s3", catalog_database_name = "frillmart", format = "glueparquet", connection_options = {"path": "s3://bond-s3-forspark/invoice-sandbox/", "partitionKeys": ["Country"], "enableUpdateCatalog":true, "updateBehavior":"UPDATE_IN_DATABASE"}, catalog_table_name = "invoice_sandbox", transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform0]
DataSink0 = glueContext.getSink(path = "s3://bond-s3-forspark/invoice-sandbox/", connection_type = "s3", updateBehavior = "UPDATE_IN_DATABASE", partitionKeys = ["Country"], enableUpdateCatalog = True, transformation_ctx = "DataSink0")
DataSink0.setCatalogInfo(catalogDatabase = "frillmart",catalogTableName = "invoice_sandbox")
DataSink0.setFormat("glueparquet")
DataSink0.writeFrame(Transform0)

job.commit()
