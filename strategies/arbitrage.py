import requests

from config import MIN_PROFIT_USD, TRADE_AMOUNT_USD


USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
SOL_MINT = "So11111111111111111111111111111111111111112"

JUPITER_QUOTE_URL = "https://api.jup.ag/swap/v1/quote"

USDC_DECIMALS = 1_000_000

# Temporary estimate for two transactions and other execution costs.
# We will improve this later using live fee information.
ESTIMATED_EXECUTION_COST_USD = 0.002


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
    starting_usdc_units = int(
        TRADE_AMOUNT_USD * USDC_DECIMALS
    )

    first_quote = get_quote(
        USDC_MINT,
        SOL_MINT,
        starting_usdc_units,
    )

    sol_received = int(first_quote["outAmount"])

    second_quote = get_quote(
        SOL_MINT,
        USDC_MINT,
        sol_received,
    )

    ending_usdc_units = int(second_quote["outAmount"])
    ending_usdc = ending_usdc_units / USDC_DECIMALS

    quoted_profit = ending_usdc - TRADE_AMOUNT_USD

    estimated_net_profit = (
        quoted_profit - ESTIMATED_EXECUTION_COST_USD
    )

    first_route = get_route_name(first_quote)
    second_route = get_route_name(second_quote)

    eligible_for_paper_trade = (
        estimated_net_profit >= MIN_PROFIT_USD
    )

    if eligible_for_paper_trade:
        decision = "🟢 TEST FURTHER"
    else:
        decision = "🔴 SKIP"

    return {
        "buy": first_route,
        "sell": second_route,
        "starting_amount": TRADE_AMOUNT_USD,
        "ending_amount": ending_usdc,
        "quoted_profit": quoted_profit,
        "estimated_cost": ESTIMATED_EXECUTION_COST_USD,
        "profit": estimated_net_profit,
        "confidence": "Live quote",
        "decision": decision,
        "eligible_for_paper_trade": eligible_for_paper_trade,
        "price_impact_1": first_quote.get(
            "priceImpactPct",
            "Unknown",
        ),
        "price_impact_2": second_quote.get(
            "priceImpactPct",
            "Unknown",
        ),
    }