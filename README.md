# Pyspark-ETL-AWS

**ETL Process Using AWS Services**

This pipeline is to build a serverless ETL pipeline to validate, transform csv dataset present in AWS s3 (Simple Storage Service)data lake. The pipeline is orchestrated by serverless AWS Step Functions with error handling. When a csv file is uploaded to S3 Bucket source folder, ETL pipeline is triggered. The pipeline will start the Glue crawler and load meta data to the Glue Database that can be consumed by the AWS Glue Job. The stepfunction can also automatically invoked by the AWS lambda whenever a new file uploaded to the source S3 bucket. 

**Prerequisites**

1. An active AWS account with programmatic access
3. AWS S3 bucket
4. CSV dataset
5. AWS Glue console access
6. AWS Step Functions console access
7. AWS Lambda access
8. AWS Athena access

**Product Versions**

1. Python 3 for AWS Lambda
2. AWS Glue version 3

**Architecture**

![image](https://github.com/ntc2818/Pyspark-ETL-AWS/assets/43464281/2924d4ae-92d9-401a-8570-0013f4ee374e)

**High level work flow**
**Repository Structure**
**Workflow Execution**

![image](https://github.com/ntc2818/Pyspark-ETL-AWS/assets/43464281/a791cbe6-793f-4c5b-b3a7-b71f772c580f)

