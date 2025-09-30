import requests
import pandas as pd
from datetime import datetime, timedelta
import os

def process_earthquake_data():

    today_date = datetime.now()
    timeDelta = timedelta(days=1)
    fifteen_days_ago = today_date - timedelta(days=15)
    start_time = fifteen_days_ago.strftime("%Y-%m-%d")
    end_time = today_date.strftime("%Y-%m-%d")

    # Define (Follow the link (ctrl + click))
    url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_time}&endtime={end_time}"

    response = requests.get(url)

    earthquakes = []
    if response.status_code == 200:
        data = response.json()

        features = data["features"]
        date = today_date.strftime("%Y_%m_%d")
        filename = f"FAKEDRIVE:\\directory\\pathway\\filename.csv" # Update this or use a config file

        for feature in features:
            properties = feature["properties"]
            geometry = feature["geometry"]
            earthquake = {
                "time": properties["time"],
                "place": properties["place"],
                "magnitude": properties["mag"],
                "longitude": geometry["coordinates"][0],
                "latitude": geometry["coordinates"][1],
                "depth": geometry["coordinates"][2],
                "file_name": filename,
            }
            earthquakes.append(earthquake)

        df = pd.DataFrame(earthquakes)

        if os.path.exists(filename):
            os.remove(filename)
            print(f"File {filename}")

        df.to_csv(filename, index=False)


def main():
    process_earthquake_data()


if __name__ == "__main__":
    main()