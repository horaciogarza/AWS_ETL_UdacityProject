# Datawarehouse project (AWS)

This project was fully created by Horacio Garza, this project access to an Amazon Redshift database on AWS, creates tables + staging tables and has an ETL process that copies information from S3 to Redshift, then with SQL queries.

## Files

Files:

 - sql_queries.py: File contaning all queries that the process will perform
 - create_tables.py: File containing the process that drop/creates tables
 - etl.py: File containing the process that copies the data from S3 to Redshift
 - dwh.cfg: File containing the credentials and all sensible data.

## How to run-it

 1. Modify/add data on dwh.cfg file.
 2. Execute create_tables.py python file.
 3. Execute etl.py python file.

## Requisites

 - Python
 - psycopg2 Python library
 - configparser Python library
 - AWS Account
