import os
from dotenv import load_dotenv

load_dotenv()

ESIOS_API_TOKEN = os.getenv("ESIOS_API_TOKEN")
ESIOS_BASE_URL = "https://api.esios.ree.es"
PENINSULA_GEO_NAME = "Península"
HEADERS = {
    "Accept": "application/json; application/vnd.esios-api-v1+json",
    "Content-Type": "application/json",
    "Host": "api.esios.ree.es",
    "x-api-key": ESIOS_API_TOKEN,
    "Cache-Control": "no-cache"
}
