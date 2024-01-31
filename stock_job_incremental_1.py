import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_null_rows
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1701087692329 = glueContext.create_dynamic_frame.from_catalog(
    database="default",
    table_name="stockdataclean_data",
    transformation_ctx="AmazonS3_node1701087692329",
)

# Script generated for node Amazon S3
AmazonS3_node1701095768703 = glueContext.create_dynamic_frame.from_catalog(
    database="stock_db",
    table_name="curated_stock_data",
    transformation_ctx="AmazonS3_node1701095768703",
)

# Script generated for node Change Schema
ChangeSchema_node1701087756208 = ApplyMapping.apply(
    frame=AmazonS3_node1701087692329,
    mappings=[
        ("date1", "string", "stock_date", "date"),
        ("close", "double", "close", "double"),
        ("dividends", "double", "dividends", "double"),
        ("high", "double", "high", "double"),
        ("low", "double", "low", "double"),
        ("open", "double", "open", "double"),
        ("stock splits", "long", "stock_splits", "long"),
        ("volume", "long", "volume", "long"),
        ("name", "string", "name", "string"),
    ],
    transformation_ctx="ChangeSchema_node1701087756208",
)

# Script generated for node Remove Null Rows
RemoveNullRows_node1701087839782 = ChangeSchema_node1701087756208.gs_null_rows()

# Script generated for node Drop Duplicates
DropDuplicates_node1701091421517 = DynamicFrame.fromDF(
    RemoveNullRows_node1701087839782.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1701091421517",
)

# Script generated for node SQL Query
SqlQuery1 = """
select ROUND(low, 2) AS low,ROUND(high, 2) as high,ROUND(close, 2) as close,ROUND(open, 2) as open,stock_date,dividends,stock_splits,volume,name from myDataSource
"""
SQLQuery_node1701087851797 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1,
    mapping={"myDataSource": DropDuplicates_node1701091421517},
    transformation_ctx="SQLQuery_node1701087851797",
)

# Script generated for node SQL Query
SqlQuery0 = """
-- select * from myDataSource
SELECT *
FROM myDataSource
LEFT JOIN myDataSource_1 
  ON myDataSource.stock_date = myDataSource_1.stock_date 
  AND myDataSource.name = myDataSource_1.name
WHERE myDataSource_1.stock_date IS NULL AND myDataSource_1.name IS NULL

"""
SQLQuery_node1701095790678 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "myDataSource_1": AmazonS3_node1701095768703,
        "myDataSource": SQLQuery_node1701087851797,
    },
    transformation_ctx="SQLQuery_node1701095790678",
)

# Script generated for node Amazon S3
AmazonS3_node1701090385279 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1701095790678,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://bdus829/stockbucketbduk1710/curated_data/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1701090385279",
)

job.commit()
