USE DATABASE SCD_DEMO;

USE SCHEMA SCD_DEMO.SCD2;

--Merge statement to insert data into the actual table based on condition
MERGE INTO customer AS c
USING customer_raw AS cr
    ON c.customer_id = cr.customer_id
WHEN matched and c.customer_id <> cr.customer_id OR
                 c.first_name  <> cr.first_name  OR
                 c.last_name   <> cr.last_name   OR
                 c.email       <> cr.email       OR
                 c.street      <> cr.street      OR
                 c.city        <> cr.city        OR
                 c.state       <> cr.state       OR
                 c.country     <> cr.country then update
     set c.customer_id = cr.customer_id ,
         c.first_name  = cr.first_name  ,
         c.last_name   = cr.last_name   ,
         c.email       = cr.email       ,
         c.street      = cr.street      ,
         c.city        = cr.city        ,
         c.state       = cr.state       ,
         c.country     = cr.country,
         update_timestamp = current_timestamp()
WHEN not matched then insert
           (c.customer_id,c.first_name,c.last_name,c.email,c.street,c.city,c.state,c.country)
     VALUES(cr.customer_id,cr.first_name,cr.last_name,cr.email,cr.street,cr.city,cr.state,cr.country);


-- Instead of running the query again and again manually we will put ti inside Stored Procedure
CREATE OR REPLACE PROCEDURE pdr_scd_demo() 
RETURNS string not null
language javascript
AS
    $$
        var cmd = `
            MERGE INTO customer AS c
            USING customer_raw AS cr
                ON c.customer_id = cr.customer_id
            WHEN matched and c.customer_id <> cr.customer_id OR
                             c.first_name  <> cr.first_name  OR
                             c.last_name   <> cr.last_name   OR
                             c.email       <> cr.email       OR
                             c.street      <> cr.street      OR
                             c.city        <> cr.city        OR
                             c.state       <> cr.state       OR
                             c.country     <> cr.country then update
                 set c.customer_id = cr.customer_id ,
                     c.first_name  = cr.first_name  ,
                     c.last_name   = cr.last_name   ,
                     c.email       = cr.email       ,
                     c.street      = cr.street      ,
                     c.city        = cr.city        ,
                     c.state       = cr.state       ,
                     c.country     = cr.country,
                     update_timestamp = current_timestamp()
            WHEN not matched then insert
                       (c.customer_id,c.first_name,c.last_name,c.email,c.street,c.city,c.state,c.country)
                 VALUES(cr.customer_id,cr.first_name,cr.last_name,cr.email,cr.street,cr.city,cr.state,cr.country);
        `
        
        var cmd1 = 'TRUNCATE TABLE SCD_DEMO.SCD2.customer_raw;'
        
        var sql = snowflake.createStatement({sqlText:cmd});
        var sql1 = snowflake.createStatement({sqlText:cmd1});

        var result = sql.execute();
        var result1 = sql1.execute();

      return cmd+'\n'+cmd1;
        
    $$;

-- manually calling stored procedure
call pdr_scd_demo();

-- To automate the above step we create Task
-- Before creating Task we need to create Users and permissions

-- Set up TASKADMIN role
USE ROLE securityadmin;
CREATE OR REPLACE ROLE taskadmin;

-- Set the active role to ACCOUNTADMIN before granting the EXECUTE TASK privilages to TASKADMIN
USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT to ROLE TASKADMIN;

-- Set the active role to SECURITYADMIN to show that this role can grant a role to another role
USE ROLE SECURITYADMIN;
GRANT ROLE taskadmin to ROLE sysadmin;
USE SYSADMIN;

-- Creating Task
CREATE OR REPLACE TASK tsk_scd_raw warehouse = COMPUTE_WH schedule = '1 minute'
ERROR_ON_NONDETERMINISTIC_MERGE = FALSE
as
call pdr_scd_demo();

SHOW TASKS;

-- After creating the Task the state of the Task will be suspended, we need to resume it

ALTER TASK tsk_scd_raw resume;


SELECT * FROM customer;
SELECT COUNT(*) FROM customer;

-- Make sure to close everything 
alter task tsk_scd_raw suspend;