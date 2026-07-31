from database.ai_ranking import (
    get_top_ai_ranked_tokens,
)
from database.token_intelligence import (
    get_top_intelligent_tokens,
)
from database.token_metrics import (
    get_liquid_tokens,
)


DEFAULT_CANDIDATE_LIMIT = 20_000


def safe_float(value):
    """
    Convert a value to float safely.
    """

    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def safe_int(value):
    """
    Convert a value to integer safely.
    """

    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def build_market_token_map(
    minimum_liquidity_usd,
    minimum_volume_24h_usd,
    limit=DEFAULT_CANDIDATE_LIMIT,
):
    """
    Return eligible market tokens indexed by mint.
    """

    tokens = get_liquid_tokens(
        minimum_liquidity_usd=(
            minimum_liquidity_usd
        ),
        minimum_volume_24h_usd=(
            minimum_volume_24h_usd
        ),
        limit=max(1, int(limit)),
    )

    return {
        token["mint"]: dict(token)
        for token in tokens
        if token.get("mint")
    }


def attach_ranking_data(
    market_token,
    ranking=None,
    intelligence=None,
):
    """
    Attach AI-ranking and intelligence details to a market token.
    """

    ranking = ranking or {}
    intelligence = intelligence or {}

    token = dict(market_token)

    market_score = safe_float(
        token.get("market_score")
    )

    intelligence_score = safe_float(
        ranking.get("intelligence_score")
        or intelligence.get("intelligence_score")
        or market_score
    )

    token.update(
        {
            "ai_opportunity_score": safe_float(
                ranking.get(
                    "ai_opportunity_score"
                )
            ),
            "raw_opportunity_score": safe_float(
                ranking.get(
                    "raw_opportunity_score"
                )
            ),
            "combined_confidence": safe_float(
                ranking.get(
                    "combined_confidence"
                )
            ),
            "risk_penalty": safe_float(
                ranking.get("risk_penalty")
            ),
            "freshness_score": safe_float(
                ranking.get("freshness_score")
            ),
            "intelligence_score": (
                intelligence_score
            ),
            "exploitation_score": safe_float(
                intelligence.get(
                    "exploitation_score"
                )
            ),
            "exploration_bonus": safe_float(
                intelligence.get(
                    "exploration_bonus"
                )
            ),
            "confidence_score": safe_float(
                intelligence.get(
                    "confidence_score"
                )
            ),
            "historical_total_scans": safe_int(
                ranking.get("total_scans")
                or intelligence.get(
                    "total_scans"
                )
            ),
            "historical_quote_success_rate": (
                safe_float(
                    intelligence.get(
                        "quote_success_rate"
                    )
                )
            ),
            "historical_eligible_rate": (
                safe_float(
                    intelligence.get(
                        "eligible_scan_rate"
                    )
                )
            ),
            "historical_average_net_profit": (
                safe_float(
                    intelligence.get(
                        "average_net_profit"
                    )
                )
            ),
            "prediction_ai_priority": safe_float(
                ranking.get(
                    "prediction_ai_priority"
                )
            ),
            "opportunity_probability": safe_float(
                ranking.get(
                    "opportunity_probability"
                )
            ),
            "expected_profit_usd": safe_float(
                ranking.get(
                    "expected_profit_usd"
                )
            ),
            "expected_profit_score": safe_float(
                ranking.get(
                    "expected_profit_score"
                )
            ),
            "trend_score": safe_float(
                ranking.get("trend_score")
            ),
            "stability_score": safe_float(
                ranking.get("stability_score")
            ),
            "downside_risk_score": safe_float(
                ranking.get(
                    "downside_risk_score"
                )
            ),
            "prediction_confidence": safe_float(
                ranking.get(
                    "prediction_confidence"
                )
            ),
            "scanner_selection_type": (
                "unassigned"
            ),
        }
    )

    return token


def load_ranked_candidate_pool(
    minimum_liquidity_usd,
    minimum_volume_24h_usd,
    limit=DEFAULT_CANDIDATE_LIMIT,
):
    """
    Load eligible tokens in the best available ranking order.

    Priority:
    1. AI opportunity rankings.
    2. Token intelligence rankings.
    3. Market score rankings.
    """

    limit = max(1, int(limit))

    market_by_mint = build_market_token_map(
        minimum_liquidity_usd=(
            minimum_liquidity_usd
        ),
        minimum_volume_24h_usd=(
            minimum_volume_24h_usd
        ),
        limit=limit,
    )

    if not market_by_mint:
        return [], "market"

    ai_records = get_top_ai_ranked_tokens(
        limit=limit,
        minimum_confidence=0,
        minimum_score=0,
    )

    ai_by_mint = {
        record["mint"]: record
        for record in ai_records
        if record.get("mint")
    }

    intelligence_records = (
        get_top_intelligent_tokens(
            limit=limit,
            minimum_confidence=0,
        )
    )

    intelligence_by_mint = {
        record["mint"]: record
        for record in intelligence_records
        if record.get("mint")
    }

    enriched_tokens = [
        attach_ranking_data(
            market_token=market_token,
            ranking=ai_by_mint.get(mint),
            intelligence=(
                intelligence_by_mint.get(mint)
            ),
        )
        for mint, market_token
        in market_by_mint.items()
    ]

    if ai_by_mint:
        source = "ai-opportunity"

        enriched_tokens.sort(
            key=lambda token: (
                safe_float(
                    token.get(
                        "ai_opportunity_score"
                    )
                ),
                safe_float(
                    token.get(
                        "expected_profit_usd"
                    )
                ),
                safe_float(
                    token.get(
                        "opportunity_probability"
                    )
                ),
                safe_float(
                    token.get(
                        "combined_confidence"
                    )
                ),
                -safe_float(
                    token.get(
                        "downside_risk_score"
                    )
                ),
                safe_float(
                    token.get(
                        "intelligence_score"
                    )
                ),
                safe_float(
                    token.get("market_score")
                ),
            ),
            reverse=True,
        )

    elif intelligence_by_mint:
        source = "intelligence"

        enriched_tokens.sort(
            key=lambda token: (
                safe_float(
                    token.get(
                        "intelligence_score"
                    )
                ),
                safe_float(
                    token.get(
                        "exploitation_score"
                    )
                ),
                safe_float(
                    token.get(
                        "confidence_score"
                    )
                ),
                safe_float(
                    token.get("market_score")
                ),
            ),
            reverse=True,
        )

    else:
        source = "market"

        enriched_tokens.sort(
            key=lambda token: (
                safe_float(
                    token.get("market_score")
                ),
                safe_float(
                    token.get("liquidity_usd")
                ),
                safe_float(
                    token.get("volume_24h_usd")
                ),
                safe_int(
                    token.get("pair_count")
                ),
            ),
            reverse=True,
        )

    return enriched_tokens, source