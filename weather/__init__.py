#from .weather_api import fetch_weather_data
import boto3
import json
import snowflake.connector
from datetime import datetime

class WeatherInitiator:
    @staticmethod
    def PipelineProcessDateGenerator():
        pipeline_process_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        return pipeline_process_date

    @staticmethod
    def get_secret(secret_name, region_name, key_value):
        """Retrieve a secret from AWS Secrets Manager."""
        client = boto3.client('secretsmanager', region_name=region_name)
        try:
            response = client.get_secret_value(SecretId=secret_name)
            secret = json.loads(response.get('SecretString', '{}'))
            return secret.get(f'{key_value}')
        except client.exceptions.ResourceNotFoundException:
            print(f"Secret '{secret_name}' not found in region '{region_name}'.")
        except client.exceptions.AccessDeniedException:
            print(f"Access denied to secret '{secret_name}'. Check IAM policy.")
        except Exception as e:
            print(f"Error retrieving secret: {e}")
        return None

    def snowflake_connection_creator(self):
        """
        Load S3 data into a Snowflake table using an external stage.

        Args:
            snowflake_config (dict): Snowflake configuration details.
            s3_bucket (str): The S3 bucket name.
            s3_key (str): The key (path) of the file in the S3 bucket.
        """
        try:
            conn = snowflake.connector.connect(
                account=self.get_secret("sf_configuration", "us-east-2", "SNOWFLAKE_ACCOUNT"),
                warehouse=self.get_secret("sf_configuration", "us-east-2", "SNOWFLAKE_WAREHOUSE"),
                database=self.get_secret("sf_configuration", "us-east-2", "SNOWFLAKE_DATABASE"),
                schema=self.get_secret("sf_configuration", "us-east-2", "SNOWFLAKE_SCHEMA"),
                user=self.get_secret("sf_configuration", "us-east-2", "SNOWFLAKE_USER"),
                password=self.get_secret("sf_configuration", "us-east-2", "SNOWFLAKE_PASSWORD")

            )
            cursor = conn.cursor()
            return cursor

        except Exception as e:
            print(f"Error loading data into Snowflake: {e}")
