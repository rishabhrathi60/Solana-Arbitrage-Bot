import requests

USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
SOL_MINT = "So11111111111111111111111111111111111111112"

JUPITER_QUOTE_URL = "https://api.jup.ag/swap/v1/quote"


def get_quote(input_mint, output_mint, amount):
    settings = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": amount,
        "slippageBps": 50
    }

    response = requests.get(
        JUPITER_QUOTE_URL,
        params=settings,
        timeout=15
    )

    response.raise_for_status()

    return response.json()


try:
    first_quote = get_quote(
        USDC_MINT,
        SOL_MINT,
        1_000_000
    )

    sol_received = int(first_quote["outAmount"])

    second_quote = get_quote(
        SOL_MINT,
        USDC_MINT,
        sol_received
    )

    usdc_received_smallest = int(second_quote["outAmount"])
    usdc_received = usdc_received_smallest / 1_000_000

    profit_or_loss = usdc_received - 1.00
    minimum_profit = 0.02

    print("==============================")
    print("ROUND-TRIP TEST")
    print("==============================")

    print(f"Starting amount:  $1.000000 USDC")
    print(f"Ending amount:    ${usdc_received:.6f} USDC")
    print(f"Profit or loss:   ${profit_or_loss:.6f}")

    if profit_or_loss >= minimum_profit:
        print("Result: Large enough for further testing")
    else:
        print("Result: Skip this opportunity")
        print("Reason: Profit is too small to cover real trading costs")

    print("\nNo real trade was made.")

except requests.RequestException as error:
    print("The quote test failed.")
    print("Error:", error)

except KeyError as error:
    print("Jupiter returned missing information.")
    print("Missing:", error)