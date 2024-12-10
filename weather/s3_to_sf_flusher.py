from weather import WeatherInitiator as wi
import weather


class SnowflakeServiceHandler:

    @staticmethod
    def s3_to_sf_flusher(s3_key, snowflake_stage, target_table):
        """
        Flush data from S3 to a Snowflake table.

        Args:
            s3_bucket (str): The S3 bucket name.
            s3_key (str): The key (path) of the file in the S3 bucket.
            snowflake_stage (str): Snowflake stage name (e.g., '@my_snowflake_stage').
            target_table (str): Target Snowflake table (e.g., 'MY_SCHEMA.MY_TABLE').
        """
        #sf_service = None
        try:
            # Initialize the Snowflake connection
            weather_var = weather.WeatherInitiator()
            sf_service = weather_var.snowflake_connection_creator()

            if not sf_service:
                raise ConnectionError("Failed to create Snowflake connection.")

            # Verify Snowflake connection
            print("Connected to Snowflake. Listing tables:")
            #sf_service.execute("SHOW TABLES")
            #tables = sf_service.fetchall()
            #print(f"Tables: {tables}")

            # Generate COPY INTO command
            copy_command = f"""
                COPY INTO LANDING_ZONE.LZ_WEATHER_SCHEMA.LZ_WEATHER_TBL
                FROM @LANDING_ZONE.LZ_WEATHER_SCHEMA.LANDING_STAGE
                FILE_FORMAT = (
                    TYPE = 'CSV'
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                    SKIP_HEADER = 1
              )
              ON_ERROR = 'SKIP_FILE';
            """
            print(f"Executing Snowflake COPY command:\n{copy_command}")

            # Execute the COPY command
            sf_service.execute(copy_command)
            print(f"Data successfully copied from S3 to Snowflake table: {target_table}")

        except Exception as e:
            print(f"Error uploading data from S3 to Snowflake: {e}")

        finally:
            # Ensure the connection is closed
            if sf_service:
                sf_service.close()
                print("Snowflake connection closed.")


if __name__ == "__main__":
    service = SnowflakeServiceHandler()
    service.s3_to_sf_flusher("landing_zone/weather/", "landing_stage", "LANDING_ZONE.LZ_WEATHER_SCHEMA.LZ_WEATHER_TBL")

