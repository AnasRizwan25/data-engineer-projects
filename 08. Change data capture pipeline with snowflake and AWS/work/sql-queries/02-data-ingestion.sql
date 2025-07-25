-- Ingesting Data in the tables

USE DATABASE SCD_DEMO;

USE SCHEMA SCD_DEMO.SCD2
--role arn: arn:aws:iam::767398088866:role/S3_INTERGRATION_ROLE

-- giving privileges
USE ROLE ACCOUNTADMIN;
GRANT CREATE INTEGRATION ON ACCOUNT TO SYSADMIN;
USE ROLE SYSADMIN;

-- Creting Storage Integration
CREATE OR REPLACE STORAGE INTEGRATION s3_init_real_time
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::767398088866:role/S3_INTERGRATION_ROLE'
STORAGE_ALLOWED_LOCATIONS = ('s3://bucket-name/');

-- giving privileges
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON INTEGRATION s3_init_real_time TO ROLE SYSADMIN;
USE ROLE SYSADMIN;

DESC STORAGE INTEGRATION s3_init_real_time;

-- Creating Stage
CREATE OR REPLACE STAGE customer_ext_stage
url = 's3://bucket-name/'
STORAGE_INTEGRATION = s3_init_real_time;

SHOW STAGES;
LIST @customer_ext_stage;

-- Creating File Format
CREATE OR REPLACE FILE FORMAT csv
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1;

-- Creating Snowpipe
CREATE OR REPLACE PIPE customer_s3_pipe
AUTO_INGEST = TRUE
AS
COPY INTO CUSTOMER_RAW
FROM @customer_ext_stage
FILE_FORMAT = CSV;

select * from customer_raw;

-- Check pipes details to get the notification channel
SHOW PIPES;

-- Check current status of pipe
SELECT SYSTEM$PIPE_STATUS('customer_s3_pipe');

TRUNCATE TABLE customer_raw;

SELECT COUNT(*) FROM customer_raw;

SELECT * FROM customer_raw;