import pytest 
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import configparser
import logging
import allure

def read_config(config_file='config.ini'):
    """Reads configuration from the given file."""
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

def create_snowflake_engine(user, authenticator, account, warehouse, database, schema, role):
    """ Creates a SQLAlchemy engine for Snowflake. """
    try:
        # Create the SQLAlchemy engine and define the connection URL
        engine = create_engine(URL(
            user=user,
            authenticator=authenticator,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role
        ))

        print("Successfully created SQLAlchemy engine for Snowflake")
        return engine
    except Exception as e:
        print(f"Error creating SQLAlchemy engine for Snowflake: {e}")
        raise

@pytest.fixture(scope='module') # 'module' as we want to reuse the database connection for all tests within this single test file
def snowflake_connection():
    """Fixture to set up a Snowflake connection and clean up after tests."""

    # Read configuration
    config = read_config()

    # Define Snowflake connection parameters
    user = config['database']['user']
    authenticator = 'externalbrowser' # For AzureADSSO
    account = config['database']['account']
    warehouse = config['database']['warehouse']
    database = config['database']['database']
    schema = config['table']['schema']
    role = config['user']['role']

    # Create the SQLAlchemy engine
    engine = create_snowflake_engine(user, authenticator, account, warehouse, database, schema, role)
    try:
        connection = engine.connect()
        # Yield the connection to the test function
        yield connection
    
    finally: # Resource Cleanup: Ensures that the connection is closed and the engine is disposed of, no matter what happens in the try block or the test function.
    # Make certain to close the connection by executing connection.close() before engine.dispose(); 
    # otherwise, the Python Garbage collector removes the resources required to communicate with Snowflake, 
    # preventing the Python connector from closing the session properly.
        connection.close()
        engine.dispose()

def count_rows_in_table(connection, table_name):
    """Counts the number of rows in a Snowflake table."""
    try:
        query = f"SELECT COUNT(*) FROM {table_name};"
        df = pd.read_sql(query, connection)
        row_count = df.iloc[0, 0]
        return row_count
    except Exception as e:
        print(f"Error counting rows in table {table_name}: {e}")
        raise

def test_row_count_in_table(snowflake_connection):
    """Test to verify the row count in a specific Snowflake table."""

    config = read_config()
    table_name = config['table']['table_name']

    try:
        row_count = count_rows_in_table(snowflake_connection, table_name)
        assert row_count == 1876, f"Expected 1876 rows in table {table_name}, but found {row_count}"
        print(f"Row count test passed for table {table_name}")
    except Exception as e:
        print(f"Test failed due to an error: {e}")
        assert False, f"Test failed due to an error: {e}"


###################################pandas.read_sql##############################################3

# import pytest # testing framework
# import pandas as pd
# import snowflake.connector # The Snowflake Python Connector for interacting with Snowflake databases.
# import configparser
# import allure 

# def connect_to_snowflake(user, authenticator, account, warehouse, database, schema, role):
#     """ Connects to the Snowflake database. """
#     try: 
#         # Establishes the connection
#         conn = snowflake.connector.connect(
#             user=user,
#             authenticator=authenticator,
#             account=account,
#             warehouse=warehouse,
#             database=database,
#             schema=schema,
#             # table_name = table_name, 
#             role=role
#         )
#         return conn
#     except Exception as e:
#         print("Error connecting to Snowflake:", str(e))
#         return None


# def read_config():
#     # Read configuration
#     config = configparser.ConfigParser()
#     config.read('config.ini')
#     return config

# @pytest.fixture
# def snowflake_connection():
#     # Read configuration
#     config = read_config()

#     # Define Snowflake connection parameters
#     user = config['database']['user']
#     authenticator = 'externalbrowser' # For AzureADSSO
#     account = config['database']['account']
#     warehouse = config['database']['warehouse']
#     database = config['database']['database']
#     schema = config['table']['schema']
#     # table_name = config['table']['table_name']
#     role = config['user']['role']

#     # Connects to Snowflake
#     conn = connect_to_snowflake(user, authenticator, account, warehouse, database, schema, role)
#     yield conn
#     conn.close()

# def snowflake_table_to_dataframe(conn, table_name):
#     """ Converts a Snowflake table to a Pandas DataFrame. """
#     try:
#         # Reads the table data into a DataFrame
#         query = f"SELECT * FROM {table_name};"
#         df = pd.read_sql(query, conn)
#         return df
#     except Exception as e:
#         print("Error reading table:", str(e))
#         return None



# def test_row_count_in_table(snowflake_connection):
#     # Read configuration
#     config = read_config()

#     table_name = config['table']['table_name']

#     try:
#         # Query to count the number of rows in the table
#         query = f"SELECT COUNT(*) FROM {table_name};"
#         df = pd.read_sql(query, snowflake_connection)
#         row_count = df.iloc[0, 0]

#         # Check if the row count is 1876
#         assert row_count == 1876, f"Expected 1861 rows in table {table_name}, but found {row_count}"
#     except Exception as e:
#         print("Error counting rows in table:", str(e))
#         assert False, f"Test failed due to an error: {str(e)}"

