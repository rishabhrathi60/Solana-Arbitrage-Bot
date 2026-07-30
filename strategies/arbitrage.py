import requests


USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
SOL_MINT = "So11111111111111111111111111111111111111112"

JUPITER_QUOTE_URL = "https://api.jup.ag/swap/v1/quote"

STARTING_USDC = 1_000_000
MINIMUM_PROFIT_USD = 0.02


def get_quote(input_mint, output_mint, amount):
    settings = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": amount,
        "slippageBps": 50,
        "restrictIntermediateTokens": "true",
    }

    response = requests.get(
        JUPITER_QUOTE_URL,
        params=settings,
        timeout=15,
    )

    response.raise_for_status()

    return response.json()


def get_route_name(quote):
    route_plan = quote.get("routePlan", [])

    if not route_plan:
        return "Unknown route"

    route_names = []

    for route_step in route_plan:
        swap_info = route_step.get("swapInfo", {})
        label = swap_info.get("label", "Unknown")

        route_names.append(label)

    return " → ".join(route_names)


def find_best_opportunity():
    first_quote = get_quote(
        USDC_MINT,
        SOL_MINT,
        STARTING_USDC,
    )

    sol_received = int(first_quote["outAmount"])

    second_quote = get_quote(
        SOL_MINT,
        USDC_MINT,
        sol_received,
    )

    ending_usdc_units = int(second_quote["outAmount"])
    ending_usdc = ending_usdc_units / 1_000_000

    profit = ending_usdc - 1.00

    first_route = get_route_name(first_quote)
    second_route = get_route_name(second_quote)

    decision = "🟢 TEST FURTHER" if profit >= MINIMUM_PROFIT_USD else "🔴 SKIP"

    return {
        "buy": first_route,
        "sell": second_route,
        "profit": profit,
        "confidence": "Live quote",
        "decision": decision,
        "ending_amount": ending_usdc,
        "price_impact_1": first_quote.get("priceImpactPct", "Unknown"),
        "price_impact_2": second_quote.get("priceImpactPct", "Unknown"),
    }