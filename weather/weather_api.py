import requests
import pandas as pd
import hashlib
from datetime import datetime
from weather import WeatherInitiator as wi

class WeatherAPIHandler:
    def fetch_weather_data(api_key, locations):
        # Define the base URL
        base_url = 'http://api.weatherstack.com/current'

        # Create a list to store weather data for each location
        weather_data_list = []

        # Loop over each location to fetch data
        for location in locations:
            # Construct the full URL
            url = f"{base_url}?access_key={api_key}&query={location}"

            # Make the API request
            response = requests.get(url)
            print(response.status_code)

            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response
                data = response.json()
                print(data)

                # Extract the desired data
                weather_data = {
                    'Location': data['location']['name'],
                    'Country': data['location']['country'],
                    'Region': data['location']['region'],
                    'Local Time': data['location']['localtime'],
                    'Temperature (C)': data['current']['temperature'],
                    'Weather Description': data['current']['weather_descriptions'][0],
                    'Wind Speed (km/h)': data['current']['wind_speed'],
                    'Pressure (mb)': data['current']['pressure'],
                    'Humidity (%)': data['current']['humidity'],
                }

                # Append the data to the list
                weather_data_list.append(weather_data)
            else:
                print(f"Failed to retrieve data for {location}. Status code: {response.status_code}")

        # Convert the list of dictionaries to a DataFrame
        df = pd.DataFrame(weather_data_list)

        # Add a new column for the pipeline process date (current timestamp)
       # pipeline_process_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        pipeline_process_date = wi.PipelineProcessDateGenerator()
        df['Pipeline Process Date'] = pipeline_process_date

        # Add a new column for the audit digest (hash of each row's data)
        df['Audit Digest'] = df.apply(lambda row: hashlib.sha256(str(row.values).encode()).hexdigest(), axis=1)



        return df
