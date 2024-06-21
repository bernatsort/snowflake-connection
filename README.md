# Snowflake Python Connector

## Description
This repository provides a comprehensive framework for establishing and testing connections to Snowflake using Python. It includes configuration setup, connection handling via SQLAlchemy and Snowflake connectors, and AWS Secrets Manager integration for secure credential management. The repository is equipped with pytest for testing various aspects of the Snowflake database, including row counts in specified tables.

### Key Components

1. **Snowflake Connection with External Browser Authentication**
   - `test_snowflake_conn_externalbrowser.py`: Establishes a Snowflake connection using external browser authentication (AzureADSSO) and tests the row count in a specified table. Uses the `config.ini` file, which defines the connection parameters for Snowflake, allowing for easy setup and modification.

2. **Snowflake Connection with AWS Key Pair Authentication**
   - `snowflake_connection_aws_keypair.py`: Demonstrates how to securely retrieve and use private keys from AWS Secrets Manager to establish a Snowflake connection.

3. **Parameterized Testing for Multiple Tables**
   - `test_row_count_snowflake.py`: Uses pytest to verify the row count for multiple tables in Snowflake, supporting parameterized testing for flexibility and scalability.

### Usage
1. **Setup Configuration**: Edit `config.ini` with your Snowflake credentials and table details. Define the AWS region and secret names.  
2. **Run Tests**: Execute the test scripts using pytest to verify your Snowflake connections and table row counts.

This framework is ideal for developers and data engineers who need a robust solution for managing and testing Snowflake database connections in a secure and efficient manner.



