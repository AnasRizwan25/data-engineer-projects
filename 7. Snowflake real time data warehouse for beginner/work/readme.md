# Snowflake Real-Time Stock Data Pipeline

## 📊 Project Description

This project implements a real-time data ingestion and transformation pipeline using **Snowflake** and **AWS S3**. The pipeline loads stock market data in CSV format from an S3 bucket into a Snowflake `RAW` table, transforms it via a `STREAM` and `PROCEDURE`, and finally inserts it into a structured `MAIN` table for analysis.

Key components of the project:
- **Snowflake Database & Schema**: Organizes stock data using `STOCK_DB` and `STOCK_SCHEMA`.
- **External Stage & Storage Integration**: Securely connects to AWS S3 using a Snowflake external stage.
- **Pipe with Auto-Ingest**: Automatically loads new files from S3 into `STOCK_RAW` using Snowpipe.
- **Stream**: Tracks changes in the `STOCK_RAW` table to support incremental transformations.
- **Stored Procedure**: Moves transformed data from `RAW` to `MAIN` and clears the `RAW` table.
- **Task Scheduler**: Executes the procedure every minute using a Snowflake task.

This pipeline enables continuous, scalable ingestion of stock data for downstream analytics or business intelligence applications.