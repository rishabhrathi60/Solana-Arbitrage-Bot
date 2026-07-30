import requests

from jupiter_data import get_sol_quote


print("==============================")
print("RISHABH MULTI-STRATEGY BOT")
print("==============================")

try:
    quote = get_sol_quote()

    sol_smallest_units = int(quote["outAmount"])

    # SOL has 9 decimal places.
    sol_amount = sol_smallest_units / 1_000_000_000

    print("\nJUPITER TEST QUOTE")
    print("------------------------------")
    print("Spend:    1 USDC")
    print(f"Receive:  {sol_amount:.9f} SOL")
    print(f"Slippage: {quote['slippageBps']} basis points")

    print("\nSAFETY STATUS")
    print("------------------------------")
    print("Wallet connected: NO")
    print("Trade submitted:  NO")
    print("Live trading:     OFF")

except requests.RequestException as error:
    print("\nJupiter could not provide a quote.")
    print("Error:", error)

except KeyError as error:
    print("\nJupiter answered, but information was missing.")
    print("Missing information:", error)