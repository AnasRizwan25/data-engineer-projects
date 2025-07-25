-- Creating Database
CREATE DATABASE IF NOT EXISTS SCD_DEMO;

-- using the database
USE DATABASE SCD_DEMO;

-- Creating the Schema
CREATE SCHEMA IF NOT EXISTS SCD2;

-- Using the schema
USE SCHEMA SCD_DEMO.SCD2;

-- To check the tables inside the schema, there should not be data
SHOW TABLES;

-- Creating 3 tables

-- Creating the actual table
CREATE OR REPLACE TABLE customer(
    customer_id NUMBER,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    street VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    update_timestamp TIMESTAMP_NTZ default current_timestamp()
);

-- Creating the historical table
CREATE OR REPLACE TABLE customer_history(
    customer_id NUMBER,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    street VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    start_time TIMESTAMP_NTZ default current_timestamp(),
    end_time TIMESTAMP_NTZ default current_timestamp(),
    is_current BOOLEAN
);

-- Creating Staging or table where RAW data will be stored
CREATE OR REPLACE TABLE customer_raw(
    customer_id NUMBER,
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    street VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR
);

show tables;

-- Craeting Stream - which will keep track of insert, update and delete on the customer table
CREATE OR REPLACE STREAM customer_table_changes ON TABLE customer;