import requests
from .config import HEADERS, ESIOS_BASE_URL

def get_indicator_data(indicator_id: int, start: str, end: str, time_trunc: str = "hour"):
    url = f"{ESIOS_BASE_URL}/indicators/{indicator_id}"
    params = {
        "start_date": start,
        "end_date": end,
        "time_trunc": time_trunc
    }
    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error fetching data: {response.status_code}\n{response.text}")
