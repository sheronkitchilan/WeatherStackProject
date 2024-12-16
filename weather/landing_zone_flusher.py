import yaml
import boto3
import io
from datetime import datetime
from weather.s3_to_sf_flusher import SnowflakeServiceHandler
from weather_api import WeatherAPIHandler as wa
from weather import WeatherInitiator as wi
import pandas as pd


class WeatherExtractorServiceHandler:
    @staticmethod
    def upload_df_to_s3(dataframe, bucket_name, s3_key):
        """
        Upload a DataFrame as a CSV file to an S3 bucket.

        Args:
            dataframe (pd.DataFrame): The DataFrame to upload.
            bucket_name (str): The name of the S3 bucket.
            s3_key (str): The key (path) for the file in the S3 bucket.
        """
        s3_client = boto3.client("s3")
        try:
            # Convert the DataFrame to a CSV in memory
            csv_buffer = io.StringIO()
            dataframe.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)  # Move the cursor to the start of the buffer

            # Upload the CSV to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=csv_buffer.getvalue()
            )
            print(f"DataFrame successfully uploaded to s3://{bucket_name}/{s3_key}")
        except Exception as e:
            print(f"Error uploading DataFrame to S3: {e}")

    def get_service(self):
        # Load configuration
        try:
            with open("../config.yaml", "r") as config_file:
                config = yaml.safe_load(config_file)
        except Exception as e:
            print(f"Error loading configuration: {e}")
            exit(1)

        # Extract settings
        locations = config.get("locations", [])
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_file = f"{config.get('output_file')}_{timestamp}.csv"  # Safe filename
        secret_name = config.get("secret_name")
        region_name = config.get("region_name")
        s3_bucket = config.get("s3_bucket")
        s3_key = f"landing_zone/weather/{output_file}"  # Organize files in S3
        snowflake_stage = config.get("snowflake_stage")
        target_table = config.get("target_table")

        if not secret_name:
            print("Secret name is not specified in the configuration. Add 'secret_name' to config.yaml.")
            exit(1)

        # Retrieve API key
        api_key = wi.get_secret(secret_name, region_name, "api_key")
        if not api_key:
            print("Failed to retrieve the API key. Exiting...")
            exit(1)

        # Fetch and save weather data
        try:
            weather_df = wa.fetch_weather_data(api_key, locations)
            print(f"Data successfully fetched and saved to {output_file}")

            # Upload to S3
            self.upload_df_to_s3(weather_df, s3_bucket, s3_key)

            # # Upload S3 data to Snowflake
            # SnowflakeServiceHandler.s3_to_sf_flusher(
            #     s3_bucket=s3_bucket,
            #     s3_key=s3_key,
            #     snowflake_stage=snowflake_stage,
            #     target_table=target_table
            # )

        except Exception as e:
            print(f"Error fetching, saving, or uploading weather data: {e}")


if __name__ == "__main__":
    service = WeatherExtractorServiceHandler()
    service.get_service()
