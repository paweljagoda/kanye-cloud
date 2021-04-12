import urllib
import json                 # Used to load data into JSON format
from pprint import pprint   # pretty-print

url = "https://api.kanye.rest/"
response = urllib.request.urlopen(url)
print(response)

text = response.read()

json_data = json.loads(text)

pprint(json_data)