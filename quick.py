import requests

ESIOS_TOKEN = "YOUR_TOKEN_HERE"

headers = {
    "Accept": "application/json; application/vnd.esios-api-v2+json",
    "x-api-key": ESIOS_TOKEN,
    "User-Agent": "Mozilla/5.0"
}

url = "https://api.esios.ree.es/indicators/1001"

r = requests.get(url, headers=headers)
print(r.status_code)
print(r.text[:300])
