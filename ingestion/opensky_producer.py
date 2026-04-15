import pandas as pd

import requests

url = "https://opensky-network.org/api/states/all?time=1458564121&icao24=3c6444"
response = requests.get(url)

data = response.json()
print(data["states"][:5])