import os
import threading
import time
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)

import requests
from dotenv import load_dotenv

from config import MIN_PROFIT_USD, TRADE_AMOUNT_USD
from database.token_intelligence import (
    get_top_intelligent_tokens,
    refresh_token_intelligence,
)
from database.token_metrics import (
    count_scanner_tokens,
    get_liquid_tokens,
    get_scanner_tokens,
)
from database.token_universe import (
    get_token_batch,
    mark_token_scanned,
)
from strategies.arbitrage import (
    ESTIMATED_EXECUTION_COST_USD,
    JUPITER_QUOTE_URL,
    USDC_DECIMALS,
    USDC_MINT,
    get_route_name,
)


load_dotenv()

JUPITER_API_KEY = os.getenv("JUPITER_API_KEY")

if not JUPITER_API_KEY:
    raise RuntimeError(
        "JUPITER_API_KEY is missing from the .env file."
    )


# ---------------------------------------------------------
# Scanner settings
# ---------------------------------------------------------

MAXIMUM_QUOTE_ATTEMPTS = 3
REQUEST_TIMEOUT_SECONDS = 20
BUY_SELL_WAIT_SECONDS = 2

# Set this to False to use the original token universe.
USE_MARKET_FILTER = True

MINIMUM_LIQUIDITY_USD = 50_000
MINIMUM_VOLUME_24H_USD = 10_000
# ---------------------------------------------------------
# Token Intelligence Engine
# ---------------------------------------------------------

USE_TOKEN_INTELLIGENCE = True

# 80% of each scanner batch comes from the highest-ranked
# intelligence results. The remaining 20% comes from the
# persistent market rotation so new opportunities can still
# be discovered.
INTELLIGENCE_EXPLOITATION_RATIO = 0.80

# Maximum eligible tokens loaded while building the intelligent
# scanner candidate pool.
INTELLIGENCE_CANDIDATE_LIMIT = 20_000

# ---------------------------------------------------------
# Adaptive scanner rules
# ---------------------------------------------------------

SMALL_POOL_MAXIMUM = 30
MEDIUM_POOL_MAXIMUM = 250

MEDIUM_POOL_BATCH_SIZE = 50
LARGE_POOL_BATCH_SIZE = 100

# Used only when the market filter is disabled.
FALLBACK_BATCH_SIZE = 20


# ---------------------------------------------------------
# Parallel scanner settings
# ---------------------------------------------------------

PARALLEL_SCANNER_ENABLED = True

# Start safely with three workers.
SCANNER_MAX_WORKERS = 3

# Process-wide Jupiter request ceiling.
#
# Each token normally uses two requests:
# USDC -> token
# token -> USDC
#
# This conservative setting starts one request per second.
MAX_QUOTE_REQUESTS_PER_MINUTE = 60

MINIMUM_QUOTE_REQUEST_INTERVAL = (
    60.0 / MAX_QUOTE_REQUESTS_PER_MINUTE
)

# Keep this at 10 for the first parallel test.
#
# After successful testing, change it to:
#
# PARALLEL_TEST_TOKEN_LIMIT = None
PARALLEL_TEST_TOKEN_LIMIT = None


# ---------------------------------------------------------
# Thread-local HTTP and rate-limiter state
# ---------------------------------------------------------

_thread_local = threading.local()

_rate_limit_lock = threading.Lock()
_last_quote_request_at = 0.0


def get_thread_session():
    """
    Return one HTTP session for the current worker thread.

    Each worker owns its own requests.Session rather than
    sharing one session across multiple threads.
    """

    if not hasattr(_thread_local, "session"):
        session = requests.Session()

        session.headers.update(
            {
                "x-api-key": JUPITER_API_KEY,
                "Accept": "application/json",
                "User-Agent": (
                    "Solana-Arbitrage-Bot/1.0"
                ),
            }
        )

        _thread_local.session = session

    return _thread_local.session


def wait_for_quote_request_slot():
    """
    Apply a process-wide Jupiter request-start limit.

    Multiple tokens may be processed concurrently, but HTTP
    requests start at controlled intervals.
    """

    global _last_quote_request_at

    with _rate_limit_lock:
        current_time = time.monotonic()

        elapsed = (
            current_time - _last_quote_request_at
        )

        wait_seconds = max(
            0.0,
            MINIMUM_QUOTE_REQUEST_INTERVAL - elapsed,
        )

        if wait_seconds > 0:
            time.sleep(wait_seconds)

        _last_quote_request_at = time.monotonic()


def get_quote(input_mint, output_mint, amount):
    """
    Request a swap quote from Jupiter.

    Includes:
    - thread-local HTTP sessions;
    - process-wide request pacing;
    - rate-limit retries;
    - temporary server-error retries;
    - connection and timeout retries.
    """

    if not input_mint:
        raise ValueError("The input mint is missing.")

    if not output_mint:
        raise ValueError("The output mint is missing.")

    if input_mint == output_mint:
        raise ValueError(
            "The input and output mint cannot be the same."
        )

    amount = int(amount)

    if amount <= 0:
        raise ValueError(
            "The Jupiter quote amount must be greater than zero."
        )

    settings = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": amount,
        "slippageBps": 50,
        "restrictIntermediateTokens": "true",
    }

    session = get_thread_session()
    last_error = None

    for attempt in range(
        1,
        MAXIMUM_QUOTE_ATTEMPTS + 1,
    ):
        wait_for_quote_request_slot()

        try:
            response = session.get(
                JUPITER_QUOTE_URL,
                params=settings,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )

            if response.status_code == 429:
                retry_after = response.headers.get(
                    "Retry-After"
                )

                try:
                    wait_seconds = float(retry_after)
                except (TypeError, ValueError):
                    wait_seconds = 5 * attempt

                last_error = requests.HTTPError(
                    "Jupiter rate limit reached.",
                    response=response,
                )

                print(
                    "Jupiter rate limit reached. "
                    f"Waiting {wait_seconds:.0f} seconds."
                )

                if attempt < MAXIMUM_QUOTE_ATTEMPTS:
                    time.sleep(wait_seconds)
                    continue

            if 500 <= response.status_code < 600:
                wait_seconds = 2 * attempt

                last_error = requests.HTTPError(
                    "Jupiter returned a temporary server error.",
                    response=response,
                )

                print(
                    "Jupiter temporary server error. "
                    f"Waiting {wait_seconds} seconds."
                )

                if attempt < MAXIMUM_QUOTE_ATTEMPTS:
                    time.sleep(wait_seconds)
                    continue

            response.raise_for_status()

            quote = response.json()

            if not isinstance(quote, dict):
                raise ValueError(
                    "Jupiter returned an unexpected response."
                )

            if "outAmount" not in quote:
                raise ValueError(
                    "Jupiter returned no outAmount: "
                    f"{quote}"
                )

            return quote

        except (
            requests.Timeout,
            requests.ConnectionError,
        ) as error:
            last_error = error

            if attempt < MAXIMUM_QUOTE_ATTEMPTS:
                wait_seconds = 2 * attempt

                print(
                    "Jupiter connection failed on attempt "
                    f"{attempt}/{MAXIMUM_QUOTE_ATTEMPTS}. "
                    f"Waiting {wait_seconds} seconds."
                )

                time.sleep(wait_seconds)
                continue

        except requests.RequestException as error:
            last_error = error

            if attempt < MAXIMUM_QUOTE_ATTEMPTS:
                wait_seconds = 2 * attempt

                print(
                    "Jupiter request failed on attempt "
                    f"{attempt}/{MAXIMUM_QUOTE_ATTEMPTS}. "
                    f"Waiting {wait_seconds} seconds."
                )

                time.sleep(wait_seconds)
                continue

            break

    raise requests.RequestException(
        "Jupiter quote request failed after "
        f"{MAXIMUM_QUOTE_ATTEMPTS} attempts: "
        f"{last_error}"
    )


def create_quote_error_result(
    symbol,
    error,
    token=None,
):
    """
    Create a standard result when a token cannot be quoted.
    """

    token = token or {}

    return {
        "token": symbol,
        "buy_route": "Unavailable",
        "sell_route": "Unavailable",
        "starting_amount": TRADE_AMOUNT_USD,
        "ending_amount": 0.0,
        "quoted_profit": 0.0,
        "estimated_cost": (
            ESTIMATED_EXECUTION_COST_USD
        ),
        "net_profit": 0.0,
        "decision": "⚠️ QUOTE ERROR",
        "eligible": False,
        "error": str(error),
        "market_score": float(
            token.get("market_score") or 0
        ),
        "liquidity_score": float(
            token.get("liquidity_score") or 0
        ),
        "volume_score": float(
            token.get("volume_score") or 0
        ),
        "pair_score": float(
            token.get("pair_score") or 0
        ),
    }


def scan_one_token(symbol, token_mint, token=None):
    """
    Test a USDC-to-token-to-USDC round trip.
    """

    token = token or {}

    if not symbol:
        raise ValueError("Token symbol is missing.")

    if not token_mint:
        raise ValueError(
            f"The mint address is missing for {symbol}."
        )

    if token_mint == USDC_MINT:
        raise ValueError(
            "USDC cannot be scanned against itself."
        )

    starting_units = int(
        TRADE_AMOUNT_USD * USDC_DECIMALS
    )

    buy_quote = get_quote(
        USDC_MINT,
        token_mint,
        starting_units,
    )

    token_received = int(
        buy_quote["outAmount"]
    )

    if token_received <= 0:
        raise ValueError(
            f"Jupiter returned zero tokens for {symbol}."
        )

    # Allow a brief delay between the buy and sell quotes.
    time.sleep(BUY_SELL_WAIT_SECONDS)

    sell_quote = get_quote(
        token_mint,
        USDC_MINT,
        token_received,
    )

    ending_units = int(
        sell_quote["outAmount"]
    )

    ending_amount = (
        ending_units / USDC_DECIMALS
    )

    quoted_profit = (
        ending_amount - TRADE_AMOUNT_USD
    )

    estimated_net_profit = (
        quoted_profit
        - ESTIMATED_EXECUTION_COST_USD
    )

    eligible = (
        estimated_net_profit >= MIN_PROFIT_USD
    )

    return {
        "token": symbol,
        "buy_route": get_route_name(
            buy_quote
        ),
        "sell_route": get_route_name(
            sell_quote
        ),
        "starting_amount": TRADE_AMOUNT_USD,
        "ending_amount": ending_amount,
        "quoted_profit": quoted_profit,
        "estimated_cost": (
            ESTIMATED_EXECUTION_COST_USD
        ),
        "net_profit": estimated_net_profit,
        "decision": (
            "🟢 TEST FURTHER"
            if eligible
            else "🔴 SKIP"
        ),
        "eligible": eligible,
        "error": "",
        "market_score": float(
            token.get("market_score") or 0
        ),
        "liquidity_score": float(
            token.get("liquidity_score") or 0
        ),
        "volume_score": float(
            token.get("volume_score") or 0
        ),
        "pair_score": float(
            token.get("pair_score") or 0
        ),
    }


def calculate_adaptive_batch_size(
    eligible_token_count,
):
    """
    Choose scanner batch size from the eligible-token count.

    Rules:
    - 1 to 30 eligible tokens: scan all.
    - 31 to 250 eligible tokens: scan 50.
    - More than 250 eligible tokens: scan 100.
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


def remove_usdc_and_duplicates(tokens):
    """
    Remove USDC and duplicate token mints.
    """

    cleaned_tokens = []
    seen_mints = set()

    for token in tokens:
        token_mint = token.get("mint")

        if not token_mint:
            continue

        if token_mint == USDC_MINT:
            continue

        if token_mint in seen_mints:
            continue

        seen_mints.add(token_mint)
        cleaned_tokens.append(token)

    return cleaned_tokens
def add_intelligence_to_token(
    token,
    intelligence_by_mint,
):
    """
    Attach stored intelligence information to a market token.
    """

    enriched_token = dict(token)

    mint = enriched_token.get("mint")

    intelligence = (
        intelligence_by_mint.get(mint)
        or {}
    )

    enriched_token.update(
        {
            "intelligence_score": float(
                intelligence.get(
                    "intelligence_score"
                )
                or enriched_token.get(
                    "market_score"
                )
                or 0
            ),
            "exploitation_score": float(
                intelligence.get(
                    "exploitation_score"
                )
                or 0
            ),
            "exploration_bonus": float(
                intelligence.get(
                    "exploration_bonus"
                )
                or 0
            ),
            "confidence_score": float(
                intelligence.get(
                    "confidence_score"
                )
                or 0
            ),
            "historical_total_scans": int(
                intelligence.get(
                    "total_scans"
                )
                or 0
            ),
            "historical_quote_success_rate": float(
                intelligence.get(
                    "quote_success_rate"
                )
                or 0
            ),
            "historical_eligible_rate": float(
                intelligence.get(
                    "eligible_scan_rate"
                )
                or 0
            ),
            "historical_average_net_profit": float(
                intelligence.get(
                    "average_net_profit"
                )
                or 0
            ),
            "scanner_selection_type": (
                "unassigned"
            ),
        }
    )

    return enriched_token


def load_intelligent_scanner_tokens(
    target_batch_size,
):
    """
    Build an adaptive intelligence-driven scanner batch.

    Selection:
    - 80% highest intelligence scores.
    - 20% persistent exploration rotation.

    The exploration group prevents the engine from permanently
    ignoring new or historically under-tested tokens.
    """

    target_batch_size = max(
        0,
        int(target_batch_size),
    )

    if target_batch_size <= 0:
        return []

    refresh_result = refresh_token_intelligence(
        minimum_liquidity_usd=(
            MINIMUM_LIQUIDITY_USD
        ),
        minimum_volume_24h_usd=(
            MINIMUM_VOLUME_24H_USD
        ),
    )

    intelligence_records = (
        get_top_intelligent_tokens(
            limit=INTELLIGENCE_CANDIDATE_LIMIT,
            minimum_confidence=0,
        )
    )

    intelligence_by_mint = {
        record["mint"]: record
        for record in intelligence_records
        if record.get("mint")
    }

    market_tokens = get_liquid_tokens(
        minimum_liquidity_usd=(
            MINIMUM_LIQUIDITY_USD
        ),
        minimum_volume_24h_usd=(
            MINIMUM_VOLUME_24H_USD
        ),
        limit=INTELLIGENCE_CANDIDATE_LIMIT,
    )

    market_tokens = remove_usdc_and_duplicates(
        market_tokens
    )

    enriched_tokens = [
        add_intelligence_to_token(
            token,
            intelligence_by_mint,
        )
        for token in market_tokens
    ]

    enriched_tokens.sort(
        key=lambda token: (
            float(
                token.get(
                    "intelligence_score"
                )
                or 0
            ),
            float(
                token.get(
                    "exploitation_score"
                )
                or 0
            ),
            float(
                token.get(
                    "confidence_score"
                )
                or 0
            ),
            float(
                token.get("market_score")
                or 0
            ),
        ),
        reverse=True,
    )

    number_to_return = min(
        target_batch_size,
        len(enriched_tokens),
    )

    if number_to_return <= 0:
        return []

    exploitation_count = int(
        round(
            number_to_return
            * INTELLIGENCE_EXPLOITATION_RATIO
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

    exploitation_tokens = []

    for token in enriched_tokens[
        :exploitation_count
    ]:
        selected_token = dict(token)
        selected_token[
            "scanner_selection_type"
        ] = "intelligence"

        exploitation_tokens.append(
            selected_token
        )

    selected_mints = {
        token.get("mint")
        for token in exploitation_tokens
        if token.get("mint")
    }

    exploration_tokens = []

    if exploration_count > 0:
        # Request more than strictly needed because some rotating
        # tokens may already appear in the exploitation group.
        exploration_request_size = min(
            max(
                exploration_count * 4,
                exploration_count,
            ),
            max(
                len(enriched_tokens),
                exploration_count,
            ),
        )

        rotating_tokens = get_scanner_tokens(
            batch_size=exploration_request_size,
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

        for token in rotating_tokens:
            mint = token.get("mint")

            if not mint:
                continue

            if mint in selected_mints:
                continue

            enriched_token = (
                add_intelligence_to_token(
                    token,
                    intelligence_by_mint,
                )
            )

            enriched_token[
                "scanner_selection_type"
            ] = "exploration"

            exploration_tokens.append(
                enriched_token
            )

            selected_mints.add(mint)

            if (
                len(exploration_tokens)
                >= exploration_count
            ):
                break

    # If persistent rotation did not provide enough unique tokens,
    # fill the remaining exploration positions with the most
    # under-tested candidates.
    missing_exploration = (
        exploration_count
        - len(exploration_tokens)
    )

    if missing_exploration > 0:
        exploration_candidates = [
            token
            for token in enriched_tokens
            if token.get("mint")
            not in selected_mints
        ]

        exploration_candidates.sort(
            key=lambda token: (
                int(
                    token.get(
                        "historical_total_scans"
                    )
                    or 0
                ),
                -float(
                    token.get(
                        "exploration_bonus"
                    )
                    or 0
                ),
                -float(
                    token.get("market_score")
                    or 0
                ),
            )
        )

        for token in exploration_candidates[
            :missing_exploration
        ]:
            selected_token = dict(token)
            selected_token[
                "scanner_selection_type"
            ] = "exploration"

            exploration_tokens.append(
                selected_token
            )

            mint = selected_token.get("mint")

            if mint:
                selected_mints.add(mint)

    selected_tokens = (
        exploitation_tokens
        + exploration_tokens
    )

    # Final safety fill in case the eligible pool is smaller or
    # duplicate records reduced the selected batch.
    if len(selected_tokens) < number_to_return:
        for token in enriched_tokens:
            mint = token.get("mint")

            if not mint:
                continue

            if mint in selected_mints:
                continue

            selected_token = dict(token)
            selected_token[
                "scanner_selection_type"
            ] = "intelligence-fill"

            selected_tokens.append(
                selected_token
            )

            selected_mints.add(mint)

            if (
                len(selected_tokens)
                >= number_to_return
            ):
                break

    print("Token Intelligence Engine refreshed.")
    print(
        "Intelligence records saved: "
        f"{refresh_result['intelligence_records_saved']:,}"
    )
    print(
        "Intelligence exploitation tokens: "
        f"{len(exploitation_tokens):,}"
    )
    print(
        "Exploration rotation tokens: "
        f"{len(exploration_tokens):,}"
    )

    return selected_tokens[
        :number_to_return
    ]

def load_adaptive_scanner_tokens():
    """
    Load an adaptive intelligence-driven scanner batch.

    When intelligence is enabled:
    - 80% comes from the highest intelligence rankings.
    - 20% comes from persistent exploration rotation.

    When intelligence is disabled, the original smart market
    rotation remains available as a safe fallback.
    """

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
        return [], eligible_count, 0

    if USE_TOKEN_INTELLIGENCE:
        tokens = load_intelligent_scanner_tokens(
            target_batch_size
        )

        return (
            tokens[:target_batch_size],
            eligible_count,
            target_batch_size,
        )

    tokens = get_scanner_tokens(
        batch_size=target_batch_size,
        minimum_liquidity_usd=(
            MINIMUM_LIQUIDITY_USD
        ),
        minimum_volume_24h_usd=(
            MINIMUM_VOLUME_24H_USD
        ),
    )

    tokens = remove_usdc_and_duplicates(
        tokens
    )

    refill_attempts = 0
    maximum_refill_attempts = 3

    while (
        len(tokens) < target_batch_size
        and refill_attempts
        < maximum_refill_attempts
        and eligible_count > len(tokens)
    ):
        refill_attempts += 1

        missing_count = (
            target_batch_size
            - len(tokens)
        )

        extra_tokens = get_scanner_tokens(
            batch_size=missing_count,
            minimum_liquidity_usd=(
                MINIMUM_LIQUIDITY_USD
            ),
            minimum_volume_24h_usd=(
                MINIMUM_VOLUME_24H_USD
            ),
        )

        updated_tokens = (
            remove_usdc_and_duplicates(
                tokens + extra_tokens
            )
        )

        if len(updated_tokens) == len(tokens):
            break

        tokens = updated_tokens

    return (
        tokens[:target_batch_size],
        eligible_count,
        target_batch_size,
    )

def load_scanner_tokens():
    """
    Load smart-filtered tokens or the original token batch.
    """

    if USE_MARKET_FILTER:
        (
            tokens,
            eligible_count,
            target_batch_size,
        ) = load_adaptive_scanner_tokens()

        print("Market filter enabled.")
        print("Smart market scoring enabled.")
        print("Adaptive batch sizing enabled.")
        print(
            "Token intelligence ranking: "
            f"{'enabled' if USE_TOKEN_INTELLIGENCE else 'disabled'}"
        )

        if USE_TOKEN_INTELLIGENCE:
            print(
                "Intelligence selection mix: "
                f"{INTELLIGENCE_EXPLOITATION_RATIO * 100:.0f}% "
                "exploitation / "
                f"{(1 - INTELLIGENCE_EXPLOITATION_RATIO) * 100:.0f}% "
                "exploration"
            )
        print(
            f"Minimum liquidity: "
            f"${MINIMUM_LIQUIDITY_USD:,.0f}"
        )
        print(
            f"Minimum 24h volume: "
            f"${MINIMUM_VOLUME_24H_USD:,.0f}"
        )
        print(
            f"Eligible token pool: "
            f"{eligible_count:,}"
        )
        print(
            f"Adaptive target batch: "
            f"{target_batch_size:,}"
        )
        print(
            f"Useful tokens loaded: "
            f"{len(tokens):,}"
        )

        return tokens

    print(
        "Market filter disabled. "
        "Using the original rotating token batch."
    )

    tokens = get_token_batch(
        batch_size=FALLBACK_BATCH_SIZE + 1
    )

    tokens = remove_usdc_and_duplicates(tokens)

    return tokens[:FALLBACK_BATCH_SIZE]


def print_token_market_details(token):
    """
    Display stored market metrics and smart scores.
    """
    intelligence_score = float(
        token.get("intelligence_score")
        or token.get("market_score")
        or 0
    )
    exploitation_score = float(
        token.get("exploitation_score")
        or 0
    )
    exploration_bonus = float(
        token.get("exploration_bonus")
        or 0
    )
    confidence_score = float(
        token.get("confidence_score")
        or 0
    )
    historical_scans = int(
        token.get("historical_total_scans")
        or 0
    )
    selection_type = (
        token.get("scanner_selection_type")
        or "market rotation"
    )

    print(
        f"  Intelligence score: "
        f"{intelligence_score:.2f}/100"
    )
    print(
        f"  Confidence: "
        f"{confidence_score:.2f}/100"
    )
    print(
        f"  Exploitation score: "
        f"{exploitation_score:.2f}/100"
    )
    print(
        f"  Exploration bonus: "
        f"{exploration_bonus:.2f}"
    )
    print(
        f"  Historical scans: "
        f"{historical_scans}"
    )
    print(
        f"  Selection type: "
        f"{selection_type}"
    )
    market_score = float(
        token.get("market_score") or 0
    )
    liquidity_score = float(
        token.get("liquidity_score") or 0
    )
    volume_score = float(
        token.get("volume_score") or 0
    )
    pair_score = float(
        token.get("pair_score") or 0
    )

    liquidity = float(
        token.get("liquidity_usd") or 0
    )
    volume = float(
        token.get("volume_24h_usd") or 0
    )
    pair_count = int(
        token.get("pair_count") or 0
    )

    print(
        f"  Market score: "
        f"{market_score:.2f}/100"
    )
    print(
        "  Score details: "
        f"liquidity {liquidity_score:.2f}, "
        f"volume {volume_score:.2f}, "
        f"pairs {pair_score:.2f}"
    )
    print(
        f"  Liquidity: ${liquidity:,.2f}"
    )
    print(
        f"  24h volume: ${volume:,.2f}"
    )
    print(
        f"  Trading pairs: {pair_count}"
    )


def scan_token_worker(token):
    """
    Scan one token inside a worker thread.

    No SQLite writes are performed here.
    """

    symbol = (
        token.get("symbol") or "UNKNOWN"
    )
    token_mint = token.get("mint")

    try:
        result = scan_one_token(
            symbol,
            token_mint,
            token=token,
        )

    except (
        requests.RequestException,
        KeyError,
        TypeError,
        ValueError,
    ) as error:
        result = create_quote_error_result(
            symbol,
            error,
            token=token,
        )

    except Exception as error:
        result = create_quote_error_result(
            symbol,
            (
                "Unexpected worker error: "
                f"{error}"
            ),
            token=token,
        )

    return {
        "token_data": token,
        "symbol": symbol,
        "mint": token_mint,
        "result": result,
    }


def run_sequential_scanner(tokens):
    """
    Scan tokens sequentially.

    This provides an easy fallback if parallel scanning is
    temporarily disabled.
    """

    worker_results = []

    for position, token in enumerate(
        tokens,
        start=1,
    ):
        symbol = (
            token.get("symbol") or "UNKNOWN"
        )

        print(
            f"\nScanning {position}/"
            f"{len(tokens)}: {symbol}"
        )

        if USE_MARKET_FILTER:
            print_token_market_details(token)

        worker_output = scan_token_worker(token)
        result = worker_output["result"]

        print(
            f"{symbol}: net profit "
            f"${result['net_profit']:.6f} "
            f"{result['decision']}"
        )

        worker_results.append(worker_output)

    return worker_results


def run_parallel_scanner(tokens):
    """
    Scan tokens concurrently using a small worker pool.
    """

    worker_results = []

    with ThreadPoolExecutor(
        max_workers=SCANNER_MAX_WORKERS,
        thread_name_prefix="scanner-worker",
    ) as executor:
        future_to_token = {
            executor.submit(
                scan_token_worker,
                token,
            ): token
            for token in tokens
        }

        completed_count = 0

        for future in as_completed(
            future_to_token
        ):
            completed_count += 1

            token = future_to_token[future]
            symbol = (
                token.get("symbol") or "UNKNOWN"
            )

            try:
                worker_output = future.result()

            except Exception as error:
                worker_output = {
                    "token_data": token,
                    "symbol": symbol,
                    "mint": token.get("mint"),
                    "result": create_quote_error_result(
                        symbol,
                        (
                            "Worker future failed: "
                            f"{error}"
                        ),
                        token=token,
                    ),
                }

            worker_results.append(worker_output)

            print(
                f"\nCompleted {completed_count}/"
                f"{len(tokens)}: {symbol}"
            )

            if USE_MARKET_FILTER:
                print_token_market_details(token)

            result = worker_output["result"]

            print(
                f"{symbol}: net profit "
                f"${result['net_profit']:.6f} "
                f"{result['decision']}"
            )

    return worker_results


def save_scan_history(worker_results):
    """
    Save token scan success or failure from the main thread.

    Keeping SQLite writes out of worker threads reduces the
    chance of database-locking errors.
    """

    for worker_output in worker_results:
        token_mint = worker_output["mint"]
        symbol = worker_output["symbol"]
        result = worker_output["result"]

        if not token_mint:
            continue

        try:
            mark_token_scanned(
                token_mint,
                successful=(
                    result["decision"]
                    != "⚠️ QUOTE ERROR"
                ),
            )

        except Exception as error:
            print(
                "Could not update scan history "
                f"for {symbol}: {error}"
            )


def scan_all_tokens():
    """
    Load an adaptive token batch and scan it.

    Jupiter requests may run in parallel, while SQLite updates
    remain in the main thread.
    """

    tokens = load_scanner_tokens()
    results = []

    if not tokens:
        if USE_MARKET_FILTER:
            print(
                "No tokens currently meet the market "
                "filter requirements."
            )
            print(
                "Run the token-metrics updater again "
                "or temporarily lower the thresholds."
            )
        else:
            print(
                "Token universe is empty. "
                "Run the token-universe updater first."
            )

        return results

    if PARALLEL_TEST_TOKEN_LIMIT is not None:
        test_limit = max(
            1,
            int(PARALLEL_TEST_TOKEN_LIMIT),
        )

        tokens = tokens[:test_limit]

        print(
            f"Parallel test limit enabled: "
            f"{len(tokens)} tokens."
        )

    if USE_MARKET_FILTER:
        print(
            f"Loaded {len(tokens)} smart-ranked, "
            "filtered tokens from the database."
        )
    else:
        print(
            f"Loaded rotating batch of {len(tokens)} "
            "tokens from the database."
        )

    print(
        f"Parallel scanner: "
        f"{'enabled' if PARALLEL_SCANNER_ENABLED else 'disabled'}"
    )
    print(
        f"Scanner workers: "
        f"{SCANNER_MAX_WORKERS}"
    )
    print(
        "Quote request ceiling: "
        f"{MAX_QUOTE_REQUESTS_PER_MINUTE}/minute"
    )

    started_at = time.monotonic()

    if PARALLEL_SCANNER_ENABLED:
        worker_results = run_parallel_scanner(
            tokens
        )
    else:
        worker_results = run_sequential_scanner(
            tokens
        )

    save_scan_history(worker_results)

    for worker_output in worker_results:
        results.append(
            worker_output["result"]
        )

    results.sort(
        key=lambda item: (
            item["decision"]
            != "⚠️ QUOTE ERROR",
            item["net_profit"],
            item.get("market_score", 0),
        ),
        reverse=True,
    )

    successful_count = sum(
        result["decision"]
        != "⚠️ QUOTE ERROR"
        for result in results
    )

    failed_count = (
        len(results) - successful_count
    )

    eligible_count = sum(
        bool(result["eligible"])
        for result in results
    )

    elapsed_seconds = (
        time.monotonic() - started_at
    )

    tokens_per_minute = (
        len(results)
        / elapsed_seconds
        * 60
        if elapsed_seconds > 0
        else 0.0
    )

    print("\nParallel batch scan completed.")
    print(
        f"Tokens scanned: {len(results)}"
    )
    print(
        f"Successful quotes: "
        f"{successful_count}"
    )
    print(
        f"Quote errors: {failed_count}"
    )
    print(
        f"Eligible opportunities: "
        f"{eligible_count}"
    )
    print(
        f"Elapsed time: "
        f"{elapsed_seconds:.1f} seconds"
    )
    print(
        f"Scanner speed: "
        f"{tokens_per_minute:.1f} tokens/minute"
    )

    return results