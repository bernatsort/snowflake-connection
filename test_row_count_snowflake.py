import boto3
import base64
from botocore.exceptions import ClientError
from snowflake.connector import connect
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import pytest

def get_secret(secret_name, region_name):
    """Retrieve a secret from AWS Secrets Manager."""
    client = boto3.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        print(f"Error retrieving secret: {e}")
        raise e

    if 'SecretString' in get_secret_value_response:
        return get_secret_value_response['SecretString']
    else:
        return base64.b64decode(get_secret_value_response['SecretBinary'])

def load_private_key(private_key_secret, passphrase_secret):
    """Load the private key using the given passphrase."""
    return serialization.load_pem_private_key(
        private_key_secret.encode('utf-8'),
        password=passphrase_secret.encode('utf-8'),
        backend=default_backend()
    )

def serialize_private_key(private_key):
    """Serialize the private key to DER format."""
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

def create_snowflake_connection(config, private_key_bytes):
    """Create a connection to Snowflake."""
    return connect(
        user=config['user'],
        account=config['account'],
        warehouse=config['warehouse'],
        database=config['database'],
        schema=config['schema'],
        role=config['role'],
        private_key=private_key_bytes
    )


def get_row_count(conn, table_name):
    """Get the row count of a specific table."""
    query = f"SELECT COUNT(*) FROM {table_name}"
    try:
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchone()
        return result[0]
    finally:
        cur.close()

# Fixtures for pytest

@pytest.fixture(scope="module")
def snowflake_config():
    return {
        'user': 'X2ODSOMACL@BOEHRINGER-INGELHEIM.COM',
        'account': 'BI-EMEA',
        'warehouse': 'DEV_OMACL_VDW_ETL',
        'database': 'DEV_OMACL_DB',
        'schema': 'LANDING_OMACL_SCHEMA',
        'role': 'BI-AS-ATLASSIAN-P-OMACL-TEAM',
        'url': 'https://BI-EMEA.snowflakecomputing.com'
    }

@pytest.fixture(scope="module")
def private_key_bytes():
    region_name = "eu-west-1"
    secret_name_private_key = "snowflake/emea/privateKey"
    secret_name_passphrase = "snowflake/emea/passphrase"

    # Retrieve secrets
    private_key_secret = get_secret(secret_name_private_key, region_name)
    passphrase_secret = get_secret(secret_name_passphrase, region_name)

    # Load and serialize private key
    private_key = load_private_key(private_key_secret, passphrase_secret)
    return serialize_private_key(private_key)

@pytest.fixture(scope="module")
def snowflake_connection(snowflake_config, private_key_bytes):
    conn = create_snowflake_connection(snowflake_config, private_key_bytes)
    yield conn # Provide the connection to the test
    conn.close() # Ensure the connection is closed after the test

# List of tables to check
tables_to_check = [
    ('QM_AUDIT', 1876),
    # Add more tables and their expected row counts here
]

@pytest.mark.parametrize('table, expected_row_count', tables_to_check)
def test_row_count(snowflake_connection, table, expected_row_count):
    actual_row_count = get_row_count(snowflake_connection, table)
    assert actual_row_count == expected_row_count, f"Expected {expected_row_count} rows in {table}, but got {actual_row_count}"


