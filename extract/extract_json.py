import requests
import pandas as pd

def extract_weather_json_data():
    # Define the base API URL and parameters
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 40.7128,
        "longitude": -74.0060,
        "hourly": "temperature_2m"
    }

    # Fetch the data using requests
    response = requests.get(url, params=params)
    data = response.json()

    hourly_data = data["hourly"]
    df = pd.DataFrame(hourly_data)

    # Convert 'time' column to datetime
    df["time"] = pd.to_datetime(df["time"])

    print(df.head(5))
    return df