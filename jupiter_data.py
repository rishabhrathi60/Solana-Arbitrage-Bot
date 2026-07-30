import requests


JUPITER_QUOTE_URL = "https://api.jup.ag/swap/v1/quote"

# Solana token addresses
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
SOL_MINT = "So11111111111111111111111111111111111111112"


def get_sol_quote():
    settings = {
        "inputMint": USDC_MINT,
        "outputMint": SOL_MINT,

        # USDC has 6 decimal places.
        # 1,000,000 means 1 USDC.
        "amount": 1_000_000,

        # 50 means 0.5% allowed price movement.
        "slippageBps": 50
    }

    response = requests.get(
        JUPITER_QUOTE_URL,
        params=settings,
        timeout=15
    )

    response.raise_for_status()

    return response.json()