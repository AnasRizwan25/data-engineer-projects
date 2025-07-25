create database database_name;
alter user {user_name} set rsa_public_key='------BEGIN PUBLIC KEY-----';

desc user {user_name};

CREATE OR REPLACE DATABASE {database_name};
CREATE OR REPLACE SCHEMA {database_name}.{schema_name};

USE DATABASE {database_name};
USE SCHEMA {database_name}.{schema_name};

CREATE OR REPLACE TABLE {table_name} (
    value VARIANT
);

select * from {table_name};
truncate table {table_name};

