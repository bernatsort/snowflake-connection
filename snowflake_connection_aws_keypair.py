import boto3
import base64
from botocore.exceptions import ClientError
from snowflake.connector import connect
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# Define the AWS region and secret names
region_name = "eu-west-1"
secret_name_private_key = "privateKey"
secret_name_passphrase = "passphrase"

def get_secret(secret_name, region_name):
    # Create a Secrets Manager client
    client = boto3.client(service_name='secretsmanager', region_name=region_name)

    try:
        # Retrieve the secrects details from AWS
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        print(f"Error retrieving secret: {e}")
        raise e

    if 'SecretString' in get_secret_value_response:
        # Extract the secrect key 
        return get_secret_value_response['SecretString']
    else:
        return base64.b64decode(get_secret_value_response['SecretBinary'])

# Retrieve secrets
private_key_secret = get_secret(secret_name_private_key, region_name)
passphrase_secret = get_secret(secret_name_passphrase, region_name)

# Load private key
private_key = serialization.load_pem_private_key(
    private_key_secret.encode('utf-8'),
    password=passphrase_secret.encode('utf-8'), 
    backend=default_backend()
)

# Serialize private key to the format expected by the Snowflake connector
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# Define Snowflake configuration
snowflake_config = {
    'user': '',
    'account': '',
    'warehouse': '',
    'database': '',
    'schema': '',
    'role': '',
    'url': '',
    'private_key': private_key_bytes
}

# Connect to Snowflake
conn = connect(
    user=snowflake_config['user'],
    account=snowflake_config['account'],
    warehouse=snowflake_config['warehouse'],
    database=snowflake_config['database'],
    schema=snowflake_config['schema'],
    role=snowflake_config['role'],
    private_key=snowflake_config['private_key']
)

# Test the connection
try:
    cur = conn.cursor()
    cur.execute("SELECT CURRENT_TIMESTAMP")
    result = cur.fetchone()
    print("Current Timestamp:", result[0])
finally:
    cur.close()
    conn.close()


