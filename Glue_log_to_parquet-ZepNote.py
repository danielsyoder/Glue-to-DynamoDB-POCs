# Zepplin Notebook to transform log file to parititioned parquet
# https://docs.aws.amazon.com/glue/latest/dg/dev-endpoint-notebook-server-considerations.html
# In addition to role permissions, the notebook requires a DynamoDB VPCe to run.

# There are two partition options below - either partition by existing key, or add a partKey.

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
#from pyspark.sql.functions import udf
from awsglue.dynamicframe import DynamicFrame
#from pyspark.sql.functions import rand --> for partKey
#from pyspark.sql.types import IntegerType --> for partKey

glueContext = GlueContext(sc)
sourceTable = "logFileCrawledByGlue"
job = Job(glueContext)

#Modify number of lines to adjust filesize and ingestion
OUTPUT_LINES_PER_FILE = 2500000
EXPORT_PATH = "s3://bucket/prefix/pqOutScatter"

#Set up DynamicFrame in Glue, map, & clean sourceTable
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "default", table_name = sourceTable, transformation_ctx = "datasource0")
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("col0", "string", "Op", "string"), ("col1", "string", "k", "string")], transformation_ctx = "applymapping1")
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")

#Move DynamicFrame to DataFrame
writeDF = dropnullfields3.toDF()

## --> Add partKey for parition (if needed) --> uncomment and replace current writeDF statement
#writeDFpart = writeDF.withColumn("partKey", (rand()*10).cast(IntegerType()))
#writeDFpart.sortWithinPartitions("k", ascending=False).write.partitionBy("partKey").option("maxRecordsPerFile", OUTPUT_LINES_PER_FILE).mode("append").parquet(EXPORT_PATH)    

writeDF.sortWithinPartitions("k", ascending=False).write.partitionBy("Op").option("maxRecordsPerFile", OUTPUT_LINES_PER_FILE).mode("append").parquet(EXPORT_PATH)    

job.commit()
