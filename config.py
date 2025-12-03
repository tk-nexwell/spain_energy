import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


ESIOS_API_TOKEN = os.getenv("ESIOS_API_TOKEN")


@dataclass
class EsiosConfig:
    """Basic configuration for ESIOS API access."""

    base_url: str = "https://api.esios.ree.es"
    spot_indicator_id: int = 600  # default indicator


# Supported indicators and their human-readable names
INDICATORS: dict[int, str] = {
    600: "Real-time",
    612: "Marginal price Intraday market session 1",
    613: "Marginal price Intraday market session 2",
    614: "Marginal price Intraday market session 3",
}


HEADERS = {
    "Accept": "application/json; application/vnd.esios-api-v1+json",
    "Content-Type": "application/json",
}

if ESIOS_API_TOKEN:
    HEADERS["x-api-key"] = ESIOS_API_TOKEN


