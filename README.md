# Glue-to-DynamoDB-POCs
Test/POC jobs to transfer data from a Glue Catalog table to a DynamoDB.

Optimal PoC - Glue_write_to_DDB - could use any Glue indexed table as the source.

The data set used for PoC was: https://registry.opendata.aws/amazon-reviews/ - Books subset of review dataset was used for test. 

Athena Create Table As Select was used to create subset of data without null attributes or duplicate items.

The DynamoDB table uses customer_id as PartitionKey and review_id as SortKey.
