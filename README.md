# Glue-to-DynamoDB-POCs
Test/POC jobs to transfer data from a Glue Catalog table to a DynamoDB.

Glue_write_to_DDB - can use any Glue indexed table as the source, uses emr-dynamodb-hadoop-xx.xx.jar as a dependancy. https://github.com/awslabs/emr-dynamodb-connector

The data set used for PoC was: https://registry.opendata.aws/amazon-reviews/ - Books subset of review dataset was used for test. 

Athena Create Table As Select was used to create subset of data without null attributes or duplicate items.

PoC DynamoDB table uses customer_id as PartitionKey and review_id as SortKey.
