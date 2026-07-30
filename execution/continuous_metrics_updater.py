import time
from datetime import datetime

from execution.update_token_metrics import (
    MAX_WORKERS,
    METRICS_BATCH_SIZE,
    update_token_metrics,
)


# Pause briefly between completed batches.
BATCH_WAIT_SECONDS = 30

# Wait longer if an unexpected cycle-level error occurs.
ERROR_WAIT_SECONDS = 60


def format_current_time():
    """
    Return a readable timestamp for terminal output.
    """

    return datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def run_continuous_metrics_updater(
    batch_size=METRICS_BATCH_SIZE,
    max_workers=MAX_WORKERS,
    batch_wait_seconds=BATCH_WAIT_SECONDS,
    max_cycles=None,
):
    """
    Continuously update rotating batches of token metrics.

    The database layer automatically prioritizes:
    1. Tokens that have never been updated.
    2. Tokens with the oldest stored metrics.

    Set max_cycles to an integer for testing.
    Leave it as None to run continuously.
    """

    batch_size = max(1, int(batch_size))
    max_workers = max(1, int(max_workers))
    batch_wait_seconds = max(
        0,
        int(batch_wait_seconds),
    )

    if max_cycles is not None:
        max_cycles = max(1, int(max_cycles))

    cycle_number = 0

    print("=" * 65)
    print("CONTINUOUS TOKEN METRICS UPDATER")
    print("=" * 65)
    print(f"Batch size: {batch_size}")
    print(f"Worker threads: {max_workers}")
    print(
        f"Wait between batches: "
        f"{batch_wait_seconds} seconds"
    )

    if max_cycles is None:
        print("Mode: Continuous")
        print(
            "Press Ctrl+C at any time to stop safely."
        )
    else:
        print(
            f"Mode: Test run — "
            f"{max_cycles} cycle(s)"
        )

    print("=" * 65)

    try:
        while True:
            cycle_number += 1

            print(
                f"\n{'=' * 65}"
            )
            print(
                f"Starting metrics cycle "
                f"{cycle_number}"
            )
            print(
                f"Started at: "
                f"{format_current_time()}"
            )
            print(
                f"{'=' * 65}"
            )

            cycle_started_at = time.monotonic()

            try:
                update_token_metrics(
                    batch_size=batch_size,
                    max_workers=max_workers,
                )

            except Exception as error:
                print(
                    "\nUnexpected cycle-level error:"
                )
                print(f"{type(error).__name__}: {error}")
                print(
                    f"Waiting {ERROR_WAIT_SECONDS} "
                    "seconds before trying again."
                )

                time.sleep(ERROR_WAIT_SECONDS)
                continue

            cycle_elapsed_seconds = (
                time.monotonic()
                - cycle_started_at
            )

            print(
                f"\nCycle {cycle_number} completed."
            )
            print(
                f"Completed at: "
                f"{format_current_time()}"
            )
            print(
                f"Cycle duration: "
                f"{cycle_elapsed_seconds:.1f} seconds"
            )

            if (
                max_cycles is not None
                and cycle_number >= max_cycles
            ):
                print(
                    "\nRequested test cycles completed."
                )
                break

            print(
                f"\nWaiting {batch_wait_seconds} "
                "seconds before the next batch."
            )

            time.sleep(batch_wait_seconds)

    except KeyboardInterrupt:
        print(
            "\n\nContinuous metrics updater stopped "
            "safely by the user."
        )
        print(
            f"Stopped at: {format_current_time()}"
        )

    print("\nMetrics updater finished.")


if __name__ == "__main__":
    run_continuous_metrics_updater()