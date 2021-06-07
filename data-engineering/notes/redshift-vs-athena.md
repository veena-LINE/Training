Data Lake
    raw data store    
    huge storage
    S3
    Sructured/Unstructured data

    Data Base
        Tables - Meta data about data, which is stored in s3 bucket
    keeping data in s3

    Grant the permission in data lake permission

Athena 
    Query engine
    Serverless

    Link data lake to S3

Redshift
    IAM Role 
        S3 
        Athena -
        GlueConsole 
    data warehouse
    may not hold all the peta bytes data
    expensive
    external schema which link to data lake, data catalog's database

    create external table based on external schema, which create table in data lake

    Use cases
        Hot data/Freq used to be kept in Redshift storage/db
        infreq/bulk, used to be in data lake as s3 files, referenced data lake db, tables +
            redshift external schema + external table


--------

customers information
    csv file

I want this to be linked in Redshift Spectrum

1. create fodler in s3
    upload file 
2. from redshift, create external schema
    create external table with external schema linking to s3 storage
3. Grant to be done on Lake formation
     SELECT, UPDATE, CREATE... DELETE, DROP
4. IAM Access to Redshift


Athena - Data Lake formation, S3, Select, output are stored in S3
            join, complex operation, data set from lake formations
            presto - based on 
            SQL
            doesn't store data itself
            Query engine
            Serverless

            5 USD per 1 TB data scanned

Redshift DW
    Redshift DB - TBs, $$ .25 USD per hour 
    Redshift Spectrum - S3, PB, EB... + 5 USD per 1 TB data scanned
            large data set, join, complex operations, several data set
    Unifiying query access between redshift db and sprectrum, we can do join,
    enrich data,

Athena vs Redshift Spectrum 


    cost
    native storage **
    server less
    access data from different schemas/**
