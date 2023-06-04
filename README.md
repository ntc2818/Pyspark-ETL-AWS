# Pyspark-ETL-AWS
**Prerequisites**

*ETL Process Using AWS Services*

This pipeline is to build a serverless ETL pipeline to validate, transform csv dataset present in AWS s3 (Simple Storage Service)data lake. The pipeline is orchestrated by serverless AWS Step Functions with error handling. When a csv file is uploaded to S3 Bucket source folder, ETL pipeline is triggered. The pipeline will start the Glue crawler and load meta data to the Glue Database that can be consumed by the AWS Glue Job. The stepfunction can also automatically invoked by the AWS lambda whenever a new file uploaded to the source S3 bucket. 
**Product Versions**

**Architecture**
**High level work flow**
**Repository Structure**
**Workflow Execution**

![image](https://github.com/ntc2818/Pyspark-ETL-AWS/assets/43464281/a791cbe6-793f-4c5b-b3a7-b71f772c580f)

