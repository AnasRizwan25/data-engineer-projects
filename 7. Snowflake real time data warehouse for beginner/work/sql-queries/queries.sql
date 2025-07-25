-- 1. Create DB and Schema
CREATE OR REPLACE DATABASE STOCK_DB;
CREATE OR REPLACE SCHEMA STOCK_DB.STOCK_SCHEMA;

USE SCHEMA STOCK_DB.STOCK_SCHEMA;

-- 2. Tables
CREATE OR REPLACE TABLE STOCK_RAW (
    timestamp TIMESTAMP,
    company STRING,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume INTEGER
);

CREATE OR REPLACE TABLE STOCK_MAIN (
    id STRING DEFAULT UUID_STRING(),
    timestamp TIMESTAMP,
    company STRING,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume INTEGER,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Storage Integration 

CREATE OR REPLACE STORAGE INTEGRATION my_s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = '---'
STORAGE_ALLOWED_LOCATIONS = ('---');  


-- 4. Stage
CREATE OR REPLACE STAGE stock_stage
URL = '---'
STORAGE_INTEGRATION = my_s3_integration
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

DESC STORAGE INTEGRATION my_s3_integration;



-- 5. Pipe
CREATE OR REPLACE PIPE stock_pipe
AUTO_INGEST = TRUE
AS
COPY INTO STOCK_RAW
FROM @stock_stage
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);


SHOW PIPES;
DROP PIPE IF EXISTS stock_pipe;


-- 6. Stream
CREATE OR REPLACE STREAM stock_raw_stream ON TABLE STOCK_RAW;

-- 7. Procedure 
CREATE OR REPLACE PROCEDURE sp_transform_stock_data()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  INSERT INTO STOCK_MAIN (timestamp, company, open, high, low, close, volume)
  SELECT timestamp, company, open, high, low, close, volume
  FROM stock_raw_stream;

  DELETE FROM STOCK_RAW;

  RETURN 'Stock_MAIN updated. RAW cleared.';
END;
$$;

-- 8. Task
CREATE OR REPLACE TASK task_process_stock_data
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS
CALL sp_transform_stock_data();



-- 9. Activate the Task
ALTER TASK task_process_stock_data RESUME;

-- 9. Deactivate the Task
ALTER TASK task_process_stock_data SUSPEND;


select * from stock_main;
select * from stock_raw;


select count(*) from stock_main;
