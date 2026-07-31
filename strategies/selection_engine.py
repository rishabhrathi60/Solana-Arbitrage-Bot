from database.token_metrics import (
    count_scanner_tokens,
    get_scanner_tokens,
)
from database.token_universe import (
    get_token_batch,
)
from strategies.arbitrage import (
    USDC_MINT,
)
from strategies.ranking_engine import (
    attach_ranking_data,
    load_ranked_candidate_pool,
)


# ---------------------------------------------------------
# Scanner selection settings
# ---------------------------------------------------------

USE_MARKET_FILTER = True
USE_AI_RANKING = True

MINIMUM_LIQUIDITY_USD = 50_000
MINIMUM_VOLUME_24H_USD = 10_000

AI_EXPLOITATION_RATIO = 0.70
AI_CANDIDATE_LIMIT = 20_000

SMALL_POOL_MAXIMUM = 30
MEDIUM_POOL_MAXIMUM = 250

MEDIUM_POOL_BATCH_SIZE = 50
LARGE_POOL_BATCH_SIZE = 100

FALLBACK_BATCH_SIZE = 20


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


def remove_usdc_and_duplicates(tokens):
    """
    Remove USDC and duplicate token mints.
    """

    cleaned_tokens = []
    seen_mints = set()

    for token in tokens:
        mint = token.get("mint")

        if not mint:
            continue

        if mint == USDC_MINT:
            continue

        if mint in seen_mints:
            continue

        seen_mints.add(mint)
        cleaned_tokens.append(dict(token))

    return cleaned_tokens


def calculate_adaptive_batch_size(
    eligible_token_count,
):
    """
    Choose batch size from the eligible-token count.
    """

    eligible_token_count = max(
        0,
        int(eligible_token_count),
    )

    if eligible_token_count == 0:
        return 0

    if eligible_token_count <= SMALL_POOL_MAXIMUM:
        return eligible_token_count

    if eligible_token_count <= MEDIUM_POOL_MAXIMUM:
        return min(
            MEDIUM_POOL_BATCH_SIZE,
            eligible_token_count,
        )

    return min(
        LARGE_POOL_BATCH_SIZE,
        eligible_token_count,
    )


def calculate_selection_counts(
    number_to_return,
):
    """
    Split a batch into AI exploitation and exploration.
    """

    number_to_return = max(
        0,
        int(number_to_return),
    )

    if number_to_return == 0:
        return 0, 0

    exploitation_count = int(
        round(
            number_to_return
            * AI_EXPLOITATION_RATIO
        )
    )

    exploitation_count = max(
        1,
        min(
            exploitation_count,
            number_to_return,
        ),
    )

    exploration_count = (
        number_to_return
        - exploitation_count
    )

    return (
        exploitation_count,
        exploration_count,
    )


def load_exploration_tokens(
    exploration_count,
    selected_mints,
    candidate_by_mint,
):
    """
    Load persistent rotating exploration tokens.
    """

    if exploration_count <= 0:
        return []

    request_size = max(
        exploration_count * 4,
        exploration_count,
    )

    rotating_tokens = get_scanner_tokens(
        batch_size=request_size,
        minimum_liquidity_usd=(
            MINIMUM_LIQUIDITY_USD
        ),
        minimum_volume_24h_usd=(
            MINIMUM_VOLUME_24H_USD
        ),
    )

    rotating_tokens = remove_usdc_and_duplicates(
        rotating_tokens
    )

    exploration_tokens = []

    for rotating_token in rotating_tokens:
        mint = rotating_token.get("mint")

        if not mint:
            continue

        if mint in selected_mints:
            continue

        candidate = candidate_by_mint.get(
            mint
        )

        if candidate:
            selected_token = dict(candidate)
        else:
            selected_token = (
                attach_ranking_data(
                    market_token=rotating_token,
                )
            )

        selected_token[
            "scanner_selection_type"
        ] = "exploration-rotation"

        exploration_tokens.append(
            selected_token
        )

        selected_mints.add(mint)

        if (
            len(exploration_tokens)
            >= exploration_count
        ):
            break

    return exploration_tokens


def fill_missing_exploration(
    exploration_tokens,
    exploration_count,
    candidates,
    selected_mints,
):
    """
    Fill exploration slots with under-tested candidates.
    """

    missing_count = (
        exploration_count
        - len(exploration_tokens)
    )

    if missing_count <= 0:
        return

    under_tested = [
        token
        for token in candidates
        if token.get("mint")
        not in selected_mints
    ]

    under_tested.sort(
        key=lambda token: (
            safe_int(
                token.get(
                    "historical_total_scans"
                )
            ),
            -safe_float(
                token.get(
                    "exploration_bonus"
                )
            ),
            -safe_float(
                token.get("market_score")
            ),
        )
    )

    for token in under_tested[
        :missing_count
    ]:
        selected_token = dict(token)

        selected_token[
            "scanner_selection_type"
        ] = "exploration-under-tested"

        exploration_tokens.append(
            selected_token
        )

        mint = selected_token.get("mint")

        if mint:
            selected_mints.add(mint)


def load_ai_selected_tokens(
    target_batch_size,
):
    """
    Build a 70% AI-ranked and 30% exploration batch.
    """

    candidates, ranking_source = (
        load_ranked_candidate_pool(
            minimum_liquidity_usd=(
                MINIMUM_LIQUIDITY_USD
            ),
            minimum_volume_24h_usd=(
                MINIMUM_VOLUME_24H_USD
            ),
            limit=AI_CANDIDATE_LIMIT,
        )
    )

    candidates = remove_usdc_and_duplicates(
        candidates
    )

    number_to_return = min(
        max(0, int(target_batch_size)),
        len(candidates),
    )

    if number_to_return <= 0:
        return [], ranking_source

    (
        exploitation_count,
        exploration_count,
    ) = calculate_selection_counts(
        number_to_return
    )

    exploitation_tokens = []

    for token in candidates[
        :exploitation_count
    ]:
        selected_token = dict(token)

        selected_token[
            "scanner_selection_type"
        ] = (
            f"{ranking_source}-exploitation"
        )

        exploitation_tokens.append(
            selected_token
        )

    selected_mints = {
        token.get("mint")
        for token in exploitation_tokens
        if token.get("mint")
    }

    candidate_by_mint = {
        token["mint"]: token
        for token in candidates
        if token.get("mint")
    }

    exploration_tokens = (
        load_exploration_tokens(
            exploration_count=(
                exploration_count
            ),
            selected_mints=selected_mints,
            candidate_by_mint=(
                candidate_by_mint
            ),
        )
    )

    fill_missing_exploration(
        exploration_tokens=exploration_tokens,
        exploration_count=exploration_count,
        candidates=candidates,
        selected_mints=selected_mints,
    )

    selected_tokens = (
        exploitation_tokens
        + exploration_tokens
    )

    if len(selected_tokens) < number_to_return:
        for token in candidates:
            mint = token.get("mint")

            if not mint:
                continue

            if mint in selected_mints:
                continue

            selected_token = dict(token)

            selected_token[
                "scanner_selection_type"
            ] = f"{ranking_source}-fill"

            selected_tokens.append(
                selected_token
            )

            selected_mints.add(mint)

            if (
                len(selected_tokens)
                >= number_to_return
            ):
                break

    print(
        "Ranking source: "
        f"{ranking_source}"
    )

    print(
        "AI exploitation tokens: "
        f"{len(exploitation_tokens):,}"
    )

    print(
        "Exploration tokens: "
        f"{len(exploration_tokens):,}"
    )

    return (
        selected_tokens[:number_to_return],
        ranking_source,
    )


def load_market_rotation_tokens(
    target_batch_size,
):
    """
    Load the original filtered market rotation.
    """

    tokens = get_scanner_tokens(
        batch_size=target_batch_size,
        minimum_liquidity_usd=(
            MINIMUM_LIQUIDITY_USD
        ),
        minimum_volume_24h_usd=(
            MINIMUM_VOLUME_24H_USD
        ),
    )

    return remove_usdc_and_duplicates(
        tokens
    )[:target_batch_size]


def load_scanner_tokens():
    """
    Load the next scanner batch and return metadata.
    """

    if not USE_MARKET_FILTER:
        tokens = get_token_batch(
            batch_size=FALLBACK_BATCH_SIZE + 1
        )

        tokens = remove_usdc_and_duplicates(
            tokens
        )[:FALLBACK_BATCH_SIZE]

        return {
            "tokens": tokens,
            "eligible_count": len(tokens),
            "target_batch_size": len(tokens),
            "ranking_source": "token-universe",
        }

    eligible_count = count_scanner_tokens(
        minimum_liquidity_usd=(
            MINIMUM_LIQUIDITY_USD
        ),
        minimum_volume_24h_usd=(
            MINIMUM_VOLUME_24H_USD
        ),
    )

    target_batch_size = (
        calculate_adaptive_batch_size(
            eligible_count
        )
    )

    if target_batch_size <= 0:
        return {
            "tokens": [],
            "eligible_count": eligible_count,
            "target_batch_size": 0,
            "ranking_source": "none",
        }

    if USE_AI_RANKING:
        tokens, ranking_source = (
            load_ai_selected_tokens(
                target_batch_size
            )
        )
    else:
        tokens = load_market_rotation_tokens(
            target_batch_size
        )
        ranking_source = "market-rotation"

    return {
        "tokens": tokens,
        "eligible_count": eligible_count,
        "target_batch_size": (
            target_batch_size
        ),
        "ranking_source": ranking_source,
    }