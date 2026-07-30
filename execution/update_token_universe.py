import os
from pathlib import Path

import requests
from dotenv import load_dotenv

from database.token_universe import save_tokens


PROJECT_FOLDER = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_FOLDER / ".env"

JUPITER_TOKEN_URL = "https://api.jup.ag/tokens/v2/tag"
REQUEST_TIMEOUT_SECONDS = 30


def download_verified_tokens():
    """Download Jupiter's verified Solana token list."""

    load_dotenv(dotenv_path=ENV_FILE)

    api_key = os.getenv("JUPITER_API_KEY")

    if not api_key:
        raise ValueError(
            "JUPITER_API_KEY was not found in the project .env file."
        )

    response = requests.get(
        JUPITER_TOKEN_URL,
        headers={
            "x-api-key": api_key,
            "Accept": "application/json",
        },
        params={
            "query": "verified",
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    response.raise_for_status()
    token_data = response.json()

    if not isinstance(token_data, list):
        raise ValueError(
            "Jupiter returned an unexpected response instead "
            "of a token list."
        )

    cleaned_tokens = []

    for token in token_data:
        if not isinstance(token, dict):
            continue

        mint = token.get("id")
        symbol = token.get("symbol")
        name = token.get("name")
        decimals = token.get("decimals")

        if not isinstance(mint, str) or not mint.strip():
            continue

        if not isinstance(symbol, str) or not symbol.strip():
            continue

        if not isinstance(decimals, int):
            continue

        cleaned_tokens.append(
            {
                "mint": mint.strip(),
                "symbol": symbol.strip(),
                "name": (
                    name.strip()
                    if isinstance(name, str) and name.strip()
                    else symbol.strip()
                ),
                "decimals": decimals,
            }
        )

    return cleaned_tokens


def update_token_universe():
    """Download verified tokens and save them to SQLite."""

    print("Downloading verified tokens from Jupiter...")

    tokens = download_verified_tokens()

    if not tokens:
        raise ValueError(
            "Jupiter returned no usable verified tokens."
        )

    save_tokens(tokens)

    print(
        f"Successfully saved {len(tokens)} "
        "verified tokens to the database."
    )


def main():
    try:
        update_token_universe()

    except requests.HTTPError as error:
        response = error.response
        status_code = response.status_code if response else "unknown"

        print(f"Jupiter returned HTTP {status_code}.")

        if status_code == 401:
            print(
                "The API key was rejected. Check the key in .env."
            )
        elif status_code == 403:
            print(
                "The API key does not have access to this endpoint."
            )
        elif status_code == 404:
            print(
                "The Jupiter token endpoint was not found."
            )
        elif status_code == 429:
            print(
                "Jupiter's rate limit was reached. Wait before retrying."
            )
        else:
            print(error)

    except requests.RequestException as error:
        print("Could not connect to Jupiter:")
        print(error)

    except (ValueError, TypeError, KeyError) as error:
        print("Token-universe update failed:")
        print(error)


if __name__ == "__main__":
    main()