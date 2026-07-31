import time
from datetime import datetime

import requests

from database.ai_ranking import (
    refresh_ai_rankings,
)
from database.context_engine import (
    get_context_summary,
    save_context,
)
from database.decision_engine import (
    evaluate_decisions,
    get_decision_summary,
    get_top_decisions,
)
from database.opportunity_history import (
    get_opportunity_history_summary,
    get_top_opportunity_tokens,
    save_opportunity_history,
)
from database.pattern_learning import (
    update_learning,
)
from database.prediction_accuracy import (
    capture_prediction_snapshot,
    get_accuracy_by_confidence_band,
    get_prediction_accuracy_summary,
    grade_prediction_snapshot,
)
from database.reinforcement_learning import (
    get_reinforcement_summary,
    run_reinforcement_cycle,
)
from database.scanner_results import (
    save_scanner_results,
)
from database.token_intelligence import (
    refresh_token_intelligence,
)
from database.token_predictions import (
    get_prediction_summary,
    get_top_predicted_tokens,
    refresh_token_predictions,
)
from execution.paper_trader import paper_trade
from strategies.multi_token_scanner import (
    MINIMUM_LIQUIDITY_USD,
    MINIMUM_VOLUME_24H_USD,
    scan_all_tokens,
)


SCAN_INTERVAL_SECONDS = 180
ERROR_WAIT_SECONDS = 60

TOP_TOKEN_MINIMUM_SCANS = 3
TOP_TOKEN_DISPLAY_LIMIT = 5
TOP_PREDICTION_DISPLAY_LIMIT = 5
RECENT_ACCURACY_LIMIT = 500


def current_timestamp():
    return datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def safe_float(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def safe_int(value):
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def convert_scanner_result(result):
    return {
        "buy": result["buy_route"],
        "sell": result["sell_route"],
        "starting_amount": (
            result["starting_amount"]
        ),
        "ending_amount": (
            result["ending_amount"]
        ),
        "profit": result["net_profit"],
        "decision": result["decision"],
    }


def print_historical_summary():
    summary = get_opportunity_history_summary()

    print("\nHistorical opportunity summary:")
    print(
        "  Total observations: "
        f"{summary['total_observations']:,}"
    )
    print(
        "  Scanner cycles: "
        f"{safe_int(summary.get('scan_cycles')):,}"
    )
    print(
        "  Unique tokens: "
        f"{summary['unique_tokens']:,}"
    )
    print(
        "  Successful quotes: "
        f"{summary['successful_quotes']:,}"
    )
    print(
        "  Quote errors: "
        f"{summary['quote_errors']:,}"
    )
    print(
        "  Eligible observations: "
        f"{safe_int(summary.get('eligible_observations')):,}"
    )
    print(
        "  Profitable observations: "
        f"{summary['profitable_observations']:,}"
    )
    print(
        "  Average net profit: "
        f"${summary['average_net_profit']:.6f}"
    )
    print(
        "  Best net profit: "
        f"${summary['best_net_profit']:.6f}"
    )
    print(
        "  Worst net profit: "
        f"${safe_float(summary.get('worst_net_profit')):.6f}"
    )


def print_top_historical_tokens():
    top_tokens = get_top_opportunity_tokens(
        minimum_scans=TOP_TOKEN_MINIMUM_SCANS,
        limit=TOP_TOKEN_DISPLAY_LIMIT,
    )

    print(
        "\nTop historical opportunity tokens "
        f"(minimum {TOP_TOKEN_MINIMUM_SCANS} scans):"
    )

    if not top_tokens:
        print(
            "  Not enough historical observations yet."
        )
        return

    for position, token in enumerate(
        top_tokens,
        start=1,
    ):
        print(
            f"  {position}. "
            f"{token['token']} — "
            f"average "
            f"${safe_float(token.get('average_net_profit')):.6f}, "
            f"best "
            f"${safe_float(token.get('best_net_profit')):.6f}, "
            f"quote success "
            f"{safe_float(token.get('quote_success_rate')):.1f}%, "
            f"eligible "
            f"{safe_float(token.get('eligible_scan_rate')):.1f}%, "
            f"scans "
            f"{safe_int(token.get('total_scans'))}"
        )


def print_prediction_summary():
    try:
        summary = get_prediction_summary()
    except Exception as error:
        print(
            "\nPrediction summary could not be loaded."
        )
        print(error)
        return

    print("\nToken Prediction Engine summary:")
    print(
        "  Predicted tokens: "
        f"{safe_int(summary.get('total_tokens')):,}"
    )
    print(
        "  Average opportunity probability: "
        f"{safe_float(summary.get('average_opportunity_probability')):.2f}%"
    )
    print(
        "  Average expected profit: "
        f"${safe_float(summary.get('average_expected_profit_usd')):.6f}"
    )
    print(
        "  Average prediction confidence: "
        f"{safe_float(summary.get('average_prediction_confidence')):.2f}/100"
    )
    print(
        "  Average AI priority: "
        f"{safe_float(summary.get('average_ai_priority')):.2f}/100"
    )
    print(
        "  Highest AI priority: "
        f"{safe_float(summary.get('highest_ai_priority')):.2f}/100"
    )
    print(
        "  Tokens with positive expected profit: "
        f"{safe_int(summary.get('positive_expected_profit_tokens')):,}"
    )
    print(
        "  Improving tokens: "
        f"{safe_int(summary.get('improving_tokens')):,}"
    )
    print(
        "  Predictions last updated: "
        f"{summary.get('last_updated_at') or 'Not available'}"
    )


def print_top_predicted_tokens():
    try:
        top_tokens = get_top_predicted_tokens(
            limit=TOP_PREDICTION_DISPLAY_LIMIT,
            minimum_confidence=0,
        )
    except Exception as error:
        print(
            "\nTop predictions could not be loaded."
        )
        print(error)
        return

    print("\nTop predicted tokens:")

    if not top_tokens:
        print(
            "  No prediction records are available yet."
        )
        return

    for position, token in enumerate(
        top_tokens,
        start=1,
    ):
        print(
            f"  {position}. "
            f"{token.get('symbol') or 'UNKNOWN'} — "
            f"AI priority "
            f"{safe_float(token.get('ai_priority')):.2f}/100, "
            f"opportunity "
            f"{safe_float(token.get('opportunity_probability')):.2f}%, "
            f"expected profit "
            f"${safe_float(token.get('expected_profit_usd')):.6f}, "
            f"trend "
            f"{safe_float(token.get('trend_score')):.2f}/100, "
            f"stability "
            f"{safe_float(token.get('stability_score')):.2f}/100, "
            f"confidence "
            f"{safe_float(token.get('prediction_confidence')):.2f}/100"
        )


def print_prediction_accuracy_summary():
    try:
        lifetime = (
            get_prediction_accuracy_summary()
        )

        recent = get_prediction_accuracy_summary(
            recent_limit=RECENT_ACCURACY_LIMIT
        )

        bands = get_accuracy_by_confidence_band()

    except Exception as error:
        print(
            "\nPrediction Accuracy summary unavailable."
        )
        print(error)
        return

    print("\nPrediction Accuracy Engine summary:")
    print(
        "  Lifetime graded predictions: "
        f"{lifetime['total_graded_predictions']:,}"
    )
    print(
        "  Lifetime classification accuracy: "
        f"{lifetime['classification_accuracy']:.2f}%"
    )
    print(
        "  Lifetime mean probability error: "
        f"{lifetime['mean_absolute_probability_error']:.2f} points"
    )
    print(
        "  Lifetime Brier score: "
        f"{lifetime['average_brier_score']:.4f}"
    )
    print(
        "  Lifetime mean profit error: "
        f"${lifetime['mean_absolute_profit_error_usd']:.6f}"
    )
    print(
        f"  Recent {RECENT_ACCURACY_LIMIT} accuracy: "
        f"{recent['classification_accuracy']:.2f}%"
    )
    print(
        "  False positives / false negatives: "
        f"{lifetime['false_positives']:,} / "
        f"{lifetime['false_negatives']:,}"
    )
    print(
        "  Last graded: "
        f"{lifetime.get('last_graded_at') or 'Not available'}"
    )

    if bands:
        print("  Calibration by confidence band:")

        for band in bands:
            print(
                f"    {band['confidence_band']}: "
                f"{band['predictions']} predictions, "
                f"accuracy "
                f"{band['classification_accuracy']:.1f}%, "
                f"Brier "
                f"{band['average_brier_score']:.4f}"
            )


def print_context_summary():
    try:
        summary = get_context_summary()
    except Exception as error:
        print(
            "\nMarket Context summary unavailable."
        )
        print(error)
        return

    print("\nMarket Context Engine summary:")
    print(
        "  Recorded cycles: "
        f"{safe_int(summary.get('total_cycles')):,}"
    )
    print(
        "  Average market quality: "
        f"{safe_float(summary.get('avg_market_quality')):.2f}/100"
    )
    print(
        "  Average cycle profit: "
        f"${safe_float(summary.get('avg_profit')):.6f}"
    )
    print(
        "  Average quote success: "
        f"{safe_float(summary.get('avg_success')):.2f}%"
    )
    print(
        "  Average scanner speed: "
        f"{safe_float(summary.get('avg_speed')):.2f} tokens/minute"
    )


def print_decision_summary():
    try:
        summary = get_decision_summary()
        top_decisions = get_top_decisions(
            limit=5
        )
    except Exception as error:
        print(
            "\nDecision Engine summary unavailable."
        )
        print(error)
        return

    print("\nDecision Engine summary:")
    print(
        "  Operating mode: PAPER DECISION AUDIT"
    )
    print(
        "  Total decisions: "
        f"{safe_int(summary.get('total_decisions')):,}"
    )
    print(
        "  EXECUTE / WATCH / SKIP: "
        f"{safe_int(summary.get('execute_decisions')):,} / "
        f"{safe_int(summary.get('watch_decisions')):,} / "
        f"{safe_int(summary.get('skip_decisions')):,}"
    )
    print(
        "  Average decision score: "
        f"{safe_float(summary.get('average_decision_score')):.2f}/100"
    )
    print(
        "  EXECUTE profitable rate: "
        f"{safe_float(summary.get('execute_profitable_rate')):.2f}%"
    )
    print(
        "  EXECUTE average profit: "
        f"${safe_float(summary.get('execute_average_profit')):.6f}"
    )

    if top_decisions:
        print("  Top audited decisions:")

        for decision in top_decisions:
            print(
                "    "
                f"{decision.get('symbol') or 'UNKNOWN'} — "
                f"{decision.get('recommendation')} — "
                f"score "
                f"{safe_float(decision.get('decision_score')):.2f}/100, "
                f"votes "
                f"{safe_int(decision.get('votes_for_execute'))}/"
                f"{safe_int(decision.get('votes_total'))}, "
                f"expected "
                f"${safe_float(decision.get('expected_profit_usd')):.6f}"
            )

def process_paper_trades(results):
    successful_results = [
        result
        for result in results
        if result.get("decision")
        != "⚠️ QUOTE ERROR"
    ]

    if not successful_results:
        print(
            "No successful quotes were received."
        )
        return 0

    paper_trade_count = 0

    for result in successful_results:
        token = (
            result.get("token")
            or "UNKNOWN"
        )

        net_profit = safe_float(
            result.get("net_profit")
        )

        decision = (
            result.get("decision")
            or "Unknown"
        )

        market_score = safe_float(
            result.get("market_score")
        )

        intelligence_score = safe_float(
            result.get("intelligence_score")
            or market_score
        )

        print(
            f"{token}: "
            f"net profit ${net_profit:.6f} — "
            f"{decision} — "
            f"market score {market_score:.2f} — "
            f"intelligence {intelligence_score:.2f}"
        )

        if not result.get("eligible"):
            continue

        if (
            result.get("final_recommendation")
            != "EXECUTE"
        ):
            print(
                "PAPER TRADE BLOCKED BY DECISION ENGINE: "
                f"{token} — "
                f"{result.get('final_recommendation') or 'SKIP'}"
            )
            continue

        saved_trade = paper_trade(
            convert_scanner_result(result)
        )

        print(
            "PAPER TRADE SAVED: "
            f"{token}, "
            f"profit "
            f"${saved_trade['profit']:.6f}"
        )

        paper_trade_count += 1

    return paper_trade_count


def refresh_intelligence_after_cycle():
    print(
        "\nRefreshing Token Intelligence Engine "
        "with the newest scan results..."
    )

    try:
        refresh_result = (
            refresh_token_intelligence(
                minimum_liquidity_usd=(
                    MINIMUM_LIQUIDITY_USD
                ),
                minimum_volume_24h_usd=(
                    MINIMUM_VOLUME_24H_USD
                ),
            )
        )
    except Exception as error:
        print(
            "Token Intelligence Engine refresh failed."
        )
        print(error)
        return None

    print(
        "Token Intelligence Engine refreshed."
    )
    print(
        "  Market tokens processed: "
        f"{refresh_result['market_tokens_processed']:,}"
    )
    print(
        "  Intelligence records saved: "
        f"{refresh_result['intelligence_records_saved']:,}"
    )
    print(
        "  Intelligence updated at: "
        f"{refresh_result['updated_at']}"
    )

    return refresh_result


def refresh_predictions_after_cycle():
    print(
        "\nRefreshing Token Prediction Engine "
        "with the newest intelligence records..."
    )

    try:
        refresh_result = (
            refresh_token_predictions()
        )
    except Exception as error:
        print(
            "Token Prediction Engine refresh failed."
        )
        print(error)
        return None

    print(
        "Token Prediction Engine refreshed."
    )
    print(
        "  Intelligence tokens processed: "
        f"{refresh_result['intelligence_tokens_processed']:,}"
    )
    print(
        "  Prediction records saved: "
        f"{refresh_result['prediction_records_saved']:,}"
    )
    print(
        "  Predictions updated at: "
        f"{refresh_result['updated_at']}"
    )

    return refresh_result


def run_reinforcement_after_cycle(
    results,
    cycle_id,
):
    print(
        "\nEvaluating reinforcement-learning "
        "champion and challenger..."
    )

    try:
        result = run_reinforcement_cycle(
            results=results,
            cycle_id=cycle_id,
        )
    except Exception as error:
        print(
            "Reinforcement-learning evaluation failed."
        )
        print(error)
        return None

    print(
        "Reinforcement evaluation completed."
    )
    print(
        "  Models evaluated: "
        f"{result['evaluated_models']}"
    )
    print(
        "  Promotion action: "
        f"{result['promotion_action']}"
    )

    champion = result.get("champion") or {}
    challenger = result.get("challenger") or {}

    if champion:
        print(
            "  Champion: "
            f"{champion.get('model_name')} — "
            f"fitness "
            f"{safe_float(champion.get('fitness_score')):.4f}, "
            f"cycles "
            f"{safe_int(champion.get('evaluation_cycles'))}, "
            f"observations "
            f"{safe_int(champion.get('evaluation_observations'))}"
        )

    if challenger:
        print(
            "  Challenger: "
            f"{challenger.get('model_name')} — "
            f"fitness "
            f"{safe_float(challenger.get('fitness_score')):.4f}, "
            f"cycles "
            f"{safe_int(challenger.get('evaluation_cycles'))}, "
            f"observations "
            f"{safe_int(challenger.get('evaluation_observations'))}"
        )

    return result


def refresh_ai_rankings_after_cycle():
    print(
        "\nRefreshing AI Opportunity Rankings "
        "with the champion model..."
    )

    try:
        result = refresh_ai_rankings(
            minimum_liquidity_usd=(
                MINIMUM_LIQUIDITY_USD
            ),
            minimum_volume_24h_usd=(
                MINIMUM_VOLUME_24H_USD
            ),
        )
    except Exception as error:
        print(
            "AI Opportunity Ranking refresh failed."
        )
        print(error)
        return None

    print(
        "AI Opportunity Rankings refreshed."
    )
    print(
        "  Candidates processed: "
        f"{result['ranking_candidates_processed']:,}"
    )
    print(
        "  Rankings saved: "
        f"{result['ranking_records_saved']:,}"
    )
    print(
        "  Rankings updated at: "
        f"{result['updated_at']}"
    )

    return result


def print_reinforcement_summary():
    try:
        summary = get_reinforcement_summary()
    except Exception as error:
        print(
            "\nReinforcement summary unavailable."
        )
        print(error)
        return

    champion = summary.get("champion") or {}
    challenger = summary.get("challenger") or {}

    print("\nReinforcement Learning summary:")
    print(
        "  Operating mode: "
        f"{summary.get('operating_mode')}"
    )
    print(
        "  Champion model: "
        f"{champion.get('model_name') or 'Unavailable'}"
    )
    print(
        "  Champion fitness: "
        f"{safe_float(champion.get('fitness_score')):.4f}"
    )
    print(
        "  Champion observations: "
        f"{safe_int(champion.get('evaluation_observations')):,}"
    )
    print(
        "  Challenger model: "
        f"{challenger.get('model_name') or 'Not created'}"
    )
    print(
        "  Challenger fitness: "
        f"{safe_float(challenger.get('fitness_score')):.4f}"
    )


def run_one_scan_cycle():
    """
    Run one complete scanner and learning cycle.

    Prediction accuracy order is deliberately:
    capture old prediction -> scan -> grade -> learn -> refresh.
    """

    scan_time = current_timestamp()
    cycle_id = scan_time
    cycle_started_at = time.perf_counter()

    print(
        f"[{scan_time}] Capturing pre-scan predictions..."
    )

    try:
        snapshot = capture_prediction_snapshot()
        snapshot_id = snapshot["snapshot_id"]

        print(
            "Pre-scan predictions captured: "
            f"{snapshot['predictions_captured']:,}"
        )

    except Exception as error:
        snapshot_id = None

        print(
            "Prediction snapshot capture failed."
        )
        print(error)

    print(
        f"[{scan_time}] Starting token scan..."
    )

    results = scan_all_tokens()

    if not results:
        print(
            "The scanner returned no results."
        )
        return results

    latest_saved_count = (
        save_scanner_results(results)
    )

    print(
        "Latest scanner snapshot saved: "
        f"{latest_saved_count} rows."
    )

    history_saved_count = (
        save_opportunity_history(results)
    )

    print(
        "Historical scanner observations saved: "
        f"{history_saved_count} rows."
    )

    elapsed_seconds = max(
        0.001,
        time.perf_counter()
        - cycle_started_at,
    )

    try:
        save_context(
            results,
            elapsed_seconds,
        )

        print(
            "Market context snapshot saved."
        )

    except Exception as error:
        print(
            "Market context snapshot failed."
        )
        print(error)

    try:
        decision_result = evaluate_decisions(
            results=results,
            cycle_id=cycle_id,
        )

        print(
            "Decision Engine completed: "
            f"{decision_result['execute']} EXECUTE, "
            f"{decision_result['watch']} WATCH, "
            f"{decision_result['skip']} SKIP."
        )

    except Exception as error:
        print(
            "Decision Engine evaluation failed."
        )
        print(error)

        for result in results:
            result[
                "final_recommendation"
            ] = "SKIP"

    if snapshot_id:
        try:
            accuracy_result = (
                grade_prediction_snapshot(
                    snapshot_id=snapshot_id,
                    results=results,
                )
            )

            print(
                "Prediction outcomes graded: "
                f"{accuracy_result['predictions_graded']:,}"
            )

            print(
                "Unmatched scanner results: "
                f"{accuracy_result['unmatched_results']:,}"
            )

        except Exception as error:
            print(
                "Prediction grading failed."
            )
            print(error)
    else:
        print(
            "Prediction grading skipped because no "
            "pre-scan snapshot was available."
        )

    paper_trade_count = (
        process_paper_trades(results)
    )

    print(
        "Paper trades saved this cycle: "
        f"{paper_trade_count}"
    )

    intelligence_result = (
        refresh_intelligence_after_cycle()
    )

    prediction_result = None

    if intelligence_result is not None:
        prediction_result = (
            refresh_predictions_after_cycle()
        )
    else:
        print(
            "\nPrediction refresh skipped because "
            "the intelligence refresh failed."
        )

    pattern_result = update_learning(
        results
    )

    print(
        "Pattern Learning Engine rebuilt: "
        f"{safe_int(pattern_result.get('patterns_rebuilt')):,} "
        "patterns."
    )

    run_reinforcement_after_cycle(
        results=results,
        cycle_id=cycle_id,
    )

    if prediction_result is not None:
        refresh_ai_rankings_after_cycle()
    else:
        print(
            "\nAI ranking refresh skipped because "
            "predictions were not refreshed."
        )

    print_historical_summary()
    print_top_historical_tokens()
    print_prediction_summary()
    print_top_predicted_tokens()
    print_prediction_accuracy_summary()
    print_context_summary()
    print_decision_summary()
    print_reinforcement_summary()

    return results


def run_automatic_scanner():
    print("=" * 60)
    print("RISHABH AUTOMATIC PAPER SCANNER")
    print("=" * 60)
    print("Live trading: OFF")
    print("Wallet connected: NO")
    print(
        "Scan interval: "
        f"{SCAN_INTERVAL_SECONDS} seconds"
    )
    print(
        "Historical opportunity tracking: ON"
    )
    print(
        "Token Intelligence Engine: ON"
    )
    print(
        "Token Prediction Engine: ON"
    )
    print(
        "Prediction Accuracy Engine: ON"
    )
    print(
        "Market Context Engine: ON"
    )
    print(
        "Decision Engine: PAPER AUDIT"
    )
    print(
        "Learning order: "
        "SNAPSHOT → SCAN → CONTEXT → DECISION → GRADE → "
        "HISTORY → INTELLIGENCE → PREDICTIONS"
    )
    print(
        "AI opportunity ranking: ON"
    )
    print(
        "Reinforcement learning: "
        "PAPER-MODE CHAMPION/CHALLENGER"
    )
    print(
        "Live trading gates: "
        "NOT UNLOCKED"
    )
    print("Press Control + C to stop.")
    print()

    while True:
        try:
            run_one_scan_cycle()

            print(
                f"\nScan complete. Waiting "
                f"{SCAN_INTERVAL_SECONDS} seconds."
            )
            print("-" * 60)

            time.sleep(
                SCAN_INTERVAL_SECONDS
            )

        except requests.RequestException as error:
            print(
                "Internet or quote error:"
            )
            print(error)
            print(
                f"Waiting {ERROR_WAIT_SECONDS} "
                "seconds before trying again."
            )
            time.sleep(
                ERROR_WAIT_SECONDS
            )

        except (
            KeyError,
            TypeError,
            ValueError,
        ) as error:
            print(
                "The scanner received unexpected "
                "information:"
            )
            print(error)
            print(
                f"Waiting {ERROR_WAIT_SECONDS} "
                "seconds before trying again."
            )
            time.sleep(
                ERROR_WAIT_SECONDS
            )

        except Exception as error:
            print(
                "Unexpected scanner error:"
            )
            print(error)
            print(
                f"Waiting {ERROR_WAIT_SECONDS} "
                "seconds before trying again."
            )
            time.sleep(
                ERROR_WAIT_SECONDS
            )


if __name__ == "__main__":
    try:
        run_automatic_scanner()
    except KeyboardInterrupt:
        print()
        print(
            "Automatic scanner stopped safely."
        )