import pandas as pd
import requests
from urllib.request import urlopen
import certifi
import json

key = 'YUWhtiiKamutDUGfCuUeGwdNYb7zMelk'

def pull_sp500_constituents(key):
    url = f"https://financialmodelingprep.com/api/v3/sp500_constituent?apikey={key}"
    response = urlopen(url, cafile=certifi.where())
    csv_data = response.read().decode("utf-8")
    data = json.loads(csv_data)
    print(data)
    # df = pd.json_normalize(data)

pull_sp500_constituents(key)


# try:
#     # For Python 3.0 and later
#     from urllib.request import urlopen
# except ImportError:
#     # Fall back to Python 2's urllib2
#     from urllib2 import urlopen

# import certifi
# import json

# def get_jsonparsed_data(url):
#     response = urlopen(url, cafile=certifi.where())
#     data = response.read().decode("utf-8")
#     return json.loads(data)

# url = (f"https://financialmodelingprep.com/api/v3/sp500_constituent?apikey={key}")
# print(get_jsonparsed_data(url))