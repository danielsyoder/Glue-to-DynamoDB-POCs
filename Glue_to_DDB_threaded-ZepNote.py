# Zepplin Notebook to test Glue-to-DynamoDB transfer job
# https://docs.aws.amazon.com/glue/latest/dg/dev-endpoint-notebook-server-considerations.html
# In addition to role permissions, the notebook requires a DynamoDB VPCe to run.
# This has been tested successfully in Glue (not just Dev-endpoint)

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
#from pyspark.sql.functions import udf
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3
import threading
#import uuid

glueContext = GlueContext(sc)
job = Job(glueContext)

#Requires partitioned/split input sourceTable to maximize Glue executor usage
sourceTable = "glueSourceTable"

#Set number of splits/threads per DataFrame: optimize DynamoDB put consumption
numSplits = 200

#Set up DynamicFrame in Glue, map, & clean sourceTable
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "default", table_name = sourceTable, transformation_ctx = "datasource0")
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("col0", "string", "col0", "string"), ("col1", "string", "col1", "string")], transformation_ctx = "applymapping1")
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")

#Move DynamicFrame to DataFrame
dfMap = dropnullfields3.toDF()
 
#DynamoDB map/put function called against each row.
def rowToDDB(row):
    #Set mapping between columns & DynamoDB table attributes
    ddbObj = {
        "k":{"S":row.col1},
        "Op":{"S":row.col0}
        }
        
    #Initialize client & set table environment is required within the loop due to threads/locking.
    client = boto3.client("dynamodb", region_name="us-east-1")
    ddbTableOut = "GlueDDBy"    
    client.put_item(TableName=ddbTableOut, Item=ddbObj)

#Split DataFrame for threaded processing/sending to DDB
splitList = []
for i in range(numSplits): splitList.append(.1)
splitDFs = dfMap.randomSplit(splitList)

#Process each DataFrame slice  
for i in range(len(splitDFs)):
    threadFunc = splitDFs[i].foreach(rowToDDB)
    threading.Thread(target=threadFunc).start()
    
#TODO - implement thread management/failure/retry etc
#TODO - slice DF for batch_write_item: wouldn't reduce WCUs, but would optimize RPS



