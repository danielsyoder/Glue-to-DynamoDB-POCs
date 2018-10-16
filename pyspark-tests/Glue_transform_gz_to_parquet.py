## Simple ingest POC for AWS Glue transform job.
## Glue job to ingest large csv.gz (360mil rows / ~5GB) file, add a partition key, and transform to parquet.
## Designed to bypass initial crawler for simplicity and minimal overhead

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import rand
from pyspark.sql.types import IntegerType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

outputBucket = "s3://<bucketname>/<prefix>/"
sourceFile = "s3://<bucketname>/<prefix>/filename.csv.gz"

# Adjust to optimize the size of the output file
linesPerFile = 1500000 

# Load csv.gz source file
lines = sc.textFile(sourceFile)

# Split lines based on csv deliminiter
splitLines = lines.map(lambda l: l.split('|'))

# Convert RDD into DataFrame and name columns
df = spark.createDataFrame(splitLines, ['columnname1','columnname2'])

# Add a column with a random integer as a partition key - can adjust number of parititions
numPartitions = 10
dfParted = df.withColumn("pK", (rand()*numPartitions).cast(IntegerType()))

# Repartition based on pK
dfParted.repartition('pK')

# Save as compressed, sorted, and partitioned parquet files
dfParted.sortWithinPartitions("pK", ascending=False).write.partitionBy("pK").option("maxRecordsPerFile", linesPerFile).mode("append").parquet(outputBucket)     

job.commit()
