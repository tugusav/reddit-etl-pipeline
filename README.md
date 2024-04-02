# Reddit Data Pipeline using Docker, Apache Airflow, Spark, AWS S3, AWS Glue, Athena, Redshift

This project provides a data pipeline solution to extract, transform, and load (ETL) Reddit data into a Redshift data warehouse. The pipeline leverages a combination of tools and services including Apache Airflow, PostgreSQL, Amazon S3, AWS Glue, Amazon Athena, and Amazon Redshift.


## Overview

This project involves:
1. Extracting data from Reddit through API
2. Storing the data into Amazon S3 from Apache Airflow
4. Process the data on AWS Glue using Spark
5. Query the data using Athena
6. Store the processed data to Redshift

## Prerequisites
- AWS Account with appropriate permissions for S3, Glue, Athena, and Redshift.
- Reddit API credentials.
- Docker
- Python 3.9 or higher


## Setting up

1. Clone the repository 
    ```bash
    git clone https://github.com/tugusav/reddit-etl-pipeline.git
    ```
2. Create a virtual environment.
   ```bash
    python3 -m venv venv
   ```
3. Activate the virtual environment.
   ```bash
    source venv/bin/activate
   ```
4. Install the dependencies.
   ```bash
    pip install -r requirements.txt
   ```
5. Rename the configuration file and the credentials to the file.
   ```bash
    mv config/configexample.conf config/config.conf
   ```
6. Starting the containers
   ```bash
    docker-compose up -d
   ```
7. Launch the Airflow web UI.
   ```bash
    open http://localhost:8080
   ```
