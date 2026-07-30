import requests


def get_crypto_prices():
    url = "https://api.coingecko.com/api/v3/simple/price"

    settings = {
        "ids": "bitcoin,ethereum,solana",
        "vs_currencies": "usd"
    }

    response = requests.get(url, params=settings, timeout=10)
    response.raise_for_status()

    return response.json()