"""
Phase 10B — Historical Backtest Dataset Exporter

Builds, validates, and exports the normalized historical research dataset.

Pipeline:

SQLite scanner history
    ↓
HistoricalDataset
    ↓
BacktestEventCollection
    ↓
ValidationReport
    ↓
CSV / JSON / JSONL / cycle summary / manifest

This module is read-only with respect to SQLite and live trading state.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import shutil
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

try:
    from backtesting.dataset_validator import (
        DatasetValidationError,
        DatasetValidator,
        ValidationReport,
    )
    from backtesting.event_builder import (
        BacktestEvent,
        BacktestEventCollection,
        EventBuilder,
        EventBuilderError,
    )
    from backtesting.historical_dataset import (
        DEFAULT_DATABASE_PATH,
        HistoricalDataset,
        HistoricalDatasetError,
        build_historical_dataset,
    )
except ModuleNotFoundError:
    from dataset_validator import (  # type: ignore
        DatasetValidationError,
        DatasetValidator,
        ValidationReport,
    )
    from event_builder import (  # type: ignore
        BacktestEvent,
        BacktestEventCollection,
        EventBuilder,
        EventBuilderError,
    )
    from historical_dataset import (  # type: ignore
        DEFAULT_DATABASE_PATH,
        HistoricalDataset,
        HistoricalDatasetError,
        build_historical_dataset,
    )


LOGGER = logging.getLogger(__name__)

DEFAULT_EXPORT_DIRECTORY = Path("backtesting") / "exports"
DEFAULT_DATASET_CSV_NAME = "historical_backtest_events.csv"
DEFAULT_DATASET_JSON_NAME = "historical_backtest_events.json"
DEFAULT_DATASET_JSONL_NAME = "historical_backtest_events.jsonl"
DEFAULT_CYCLE_CSV_NAME = "historical_backtest_cycles.csv"
DEFAULT_VALIDATION_JSON_NAME = "validation_report.json"
DEFAULT_MANIFEST_JSON_NAME = "dataset_manifest.json"

EXPORT_SCHEMA_VERSION = "1.0.0"


class DatasetExportError(RuntimeError):
    """Base exception for dataset export failures."""


class InvalidExportConfigurationError(DatasetExportError):
    """Raised when export configuration is invalid."""


class ExportValidationFailedError(DatasetExportError):
    """Raised when validation fails and invalid exports are disallowed."""


class EmptyDatasetExportError(DatasetExportError):
    """Raised when attempting to export an empty dataset."""


@dataclass(frozen=True, slots=True)
class ExportedFile:
    label: str
    path: str
    format: str
    rows: int | None
    bytes: int
    sha256: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DatasetExportSummary:
    generated_at: datetime
    database_path: str
    output_directory: str
    schema_version: str

    dataset_rows: int
    event_rows: int
    cycles: int
    unique_assets: int
    unique_tokens: int

    successful_quotes: int
    quote_errors: int
    eligible_events: int
    profitable_events: int
    execution_candidates: int

    validation_passed: bool
    validation_errors: int
    validation_warnings: int

    files: tuple[ExportedFile, ...]

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["generated_at"] = self.generated_at.isoformat()
        return result


@dataclass(frozen=True, slots=True)
class ExportConfiguration:
    output_directory: Path = DEFAULT_EXPORT_DIRECTORY
    export_csv: bool = True
    export_json: bool = True
    export_jsonl: bool = True
    export_cycle_csv: bool = True
    export_validation_report: bool = True
    export_manifest: bool = True
    pretty_json: bool = True
    overwrite: bool = True
    allow_invalid: bool = False
    strict_loading: bool = True

    def validate(self) -> None:
        if not any(
            (
                self.export_csv,
                self.export_json,
                self.export_jsonl,
                self.export_cycle_csv,
                self.export_validation_report,
                self.export_manifest,
            )
        ):
            raise InvalidExportConfigurationError(
                "At least one export format must be enabled."
            )

        if not str(self.output_directory).strip():
            raise InvalidExportConfigurationError(
                "output_directory cannot be empty."
            )


class DatasetExporter:
    """
    Validates and exports historical backtest events atomically.

    Existing files are replaced only after a complete temporary file has been
    written successfully.
    """

    def __init__(
        self,
        configuration: ExportConfiguration | None = None,
        *,
        validator: DatasetValidator | None = None,
        event_builder: EventBuilder | None = None,
    ) -> None:
        self.configuration = configuration or ExportConfiguration()
        self.configuration.validate()
        self.validator = validator or DatasetValidator()
        self.event_builder = event_builder or EventBuilder()

    def export_database(
        self,
        database_path: str | Path = DEFAULT_DATABASE_PATH,
    ) -> DatasetExportSummary:
        database = Path(database_path)

        if not database.exists():
            raise DatasetExportError(
                f"Database does not exist: {database}"
            )

        if not database.is_file():
            raise DatasetExportError(
                f"Database path is not a file: {database}"
            )

        dataset = build_historical_dataset(
            database,
            strict=self.configuration.strict_loading,
        )
        events = self.event_builder.from_dataset(dataset)

        return self.export(
            dataset,
            events,
            database_path=database,
        )

    def export(
        self,
        dataset: HistoricalDataset,
        events: BacktestEventCollection,
        *,
        database_path: str | Path = DEFAULT_DATABASE_PATH,
    ) -> DatasetExportSummary:
        if dataset.is_empty or events.is_empty:
            raise EmptyDatasetExportError(
                "Cannot export an empty historical dataset."
            )

        report = self.validator.validate(
            dataset,
            events,
            database_path=database_path,
            strict_mode=self.configuration.strict_loading,
        )

        if not report.is_valid and not self.configuration.allow_invalid:
            raise ExportValidationFailedError(
                "Dataset validation failed. "
                "Use allow_invalid=True only for diagnostic exports."
            )

        output_directory = self.configuration.output_directory
        output_directory.mkdir(parents=True, exist_ok=True)

        exported_files: list[ExportedFile] = []

        if self.configuration.export_csv:
            exported_files.append(
                self._export_events_csv(
                    events,
                    output_directory / DEFAULT_DATASET_CSV_NAME,
                )
            )

        if self.configuration.export_json:
            exported_files.append(
                self._export_events_json(
                    events,
                    output_directory / DEFAULT_DATASET_JSON_NAME,
                )
            )

        if self.configuration.export_jsonl:
            exported_files.append(
                self._export_events_jsonl(
                    events,
                    output_directory / DEFAULT_DATASET_JSONL_NAME,
                )
            )

        if self.configuration.export_cycle_csv:
            exported_files.append(
                self._export_cycles_csv(
                    events,
                    output_directory / DEFAULT_CYCLE_CSV_NAME,
                )
            )

        if self.configuration.export_validation_report:
            exported_files.append(
                self._export_validation_report(
                    report,
                    output_directory / DEFAULT_VALIDATION_JSON_NAME,
                )
            )

        event_summary = events.summarize()

        preliminary_summary = DatasetExportSummary(
            generated_at=datetime.now(timezone.utc),
            database_path=str(database_path),
            output_directory=str(output_directory),
            schema_version=EXPORT_SCHEMA_VERSION,
            dataset_rows=len(dataset),
            event_rows=len(events),
            cycles=event_summary.total_cycles,
            unique_assets=event_summary.unique_assets,
            unique_tokens=event_summary.unique_tokens,
            successful_quotes=event_summary.successful_quotes,
            quote_errors=event_summary.quote_errors,
            eligible_events=event_summary.eligible_events,
            profitable_events=event_summary.profitable_events,
            execution_candidates=event_summary.execution_candidates,
            validation_passed=report.is_valid,
            validation_errors=report.error_count,
            validation_warnings=report.warning_count,
            files=tuple(exported_files),
        )

        if self.configuration.export_manifest:
            manifest_file = self._export_manifest(
                preliminary_summary,
                report,
                output_directory / DEFAULT_MANIFEST_JSON_NAME,
            )
            exported_files.append(manifest_file)

        return DatasetExportSummary(
            generated_at=preliminary_summary.generated_at,
            database_path=preliminary_summary.database_path,
            output_directory=preliminary_summary.output_directory,
            schema_version=preliminary_summary.schema_version,
            dataset_rows=preliminary_summary.dataset_rows,
            event_rows=preliminary_summary.event_rows,
            cycles=preliminary_summary.cycles,
            unique_assets=preliminary_summary.unique_assets,
            unique_tokens=preliminary_summary.unique_tokens,
            successful_quotes=preliminary_summary.successful_quotes,
            quote_errors=preliminary_summary.quote_errors,
            eligible_events=preliminary_summary.eligible_events,
            profitable_events=preliminary_summary.profitable_events,
            execution_candidates=preliminary_summary.execution_candidates,
            validation_passed=preliminary_summary.validation_passed,
            validation_errors=preliminary_summary.validation_errors,
            validation_warnings=preliminary_summary.validation_warnings,
            files=tuple(exported_files),
        )

    def _export_events_csv(
        self,
        events: BacktestEventCollection,
        path: Path,
    ) -> ExportedFile:
        records = events.to_records()

        if not records:
            raise EmptyDatasetExportError(
                "No event records are available for CSV export."
            )

        def writer(temp_path: Path) -> None:
            with temp_path.open(
                "w",
                encoding="utf-8",
                newline="",
            ) as handle:
                csv_writer = csv.DictWriter(
                    handle,
                    fieldnames=list(records[0].keys()),
                    extrasaction="raise",
                )
                csv_writer.writeheader()
                csv_writer.writerows(records)

        self._atomic_write(path, writer)

        return self._describe_file(
            label="events_csv",
            path=path,
            file_format="csv",
            rows=len(records),
        )

    def _export_events_json(
        self,
        events: BacktestEventCollection,
        path: Path,
    ) -> ExportedFile:
        event_summary = events.summarize()
        payload = {
            "schema_version": EXPORT_SCHEMA_VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": event_summary.to_dict(),
            "events": events.to_records(),
        }

        def writer(temp_path: Path) -> None:
            temp_path.write_text(
                json.dumps(
                    payload,
                    indent=2 if self.configuration.pretty_json else None,
                    ensure_ascii=False,
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

        self._atomic_write(path, writer)

        return self._describe_file(
            label="events_json",
            path=path,
            file_format="json",
            rows=len(events),
        )

    def _export_events_jsonl(
        self,
        events: BacktestEventCollection,
        path: Path,
    ) -> ExportedFile:
        def writer(temp_path: Path) -> None:
            with temp_path.open("w", encoding="utf-8") as handle:
                for event in events:
                    handle.write(
                        json.dumps(
                            event.to_dict(),
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                    )
                    handle.write("\n")

        self._atomic_write(path, writer)

        return self._describe_file(
            label="events_jsonl",
            path=path,
            file_format="jsonl",
            rows=len(events),
        )

    def _export_cycles_csv(
        self,
        events: BacktestEventCollection,
        path: Path,
    ) -> ExportedFile:
        records = [
            summary.to_dict()
            for summary in events.summarize_cycles()
        ]

        if not records:
            raise EmptyDatasetExportError(
                "No cycle summaries are available for export."
            )

        def writer(temp_path: Path) -> None:
            with temp_path.open(
                "w",
                encoding="utf-8",
                newline="",
            ) as handle:
                csv_writer = csv.DictWriter(
                    handle,
                    fieldnames=list(records[0].keys()),
                    extrasaction="raise",
                )
                csv_writer.writeheader()
                csv_writer.writerows(records)

        self._atomic_write(path, writer)

        return self._describe_file(
            label="cycles_csv",
            path=path,
            file_format="csv",
            rows=len(records),
        )

    def _export_validation_report(
        self,
        report: ValidationReport,
        path: Path,
    ) -> ExportedFile:
        def writer(temp_path: Path) -> None:
            temp_path.write_text(
                json.dumps(
                    report.to_dict(),
                    indent=2 if self.configuration.pretty_json else None,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

        self._atomic_write(path, writer)

        return self._describe_file(
            label="validation_report",
            path=path,
            file_format="json",
            rows=report.checks_run,
        )

    def _export_manifest(
        self,
        summary: DatasetExportSummary,
        report: ValidationReport,
        path: Path,
    ) -> ExportedFile:
        manifest = {
            "schema_version": EXPORT_SCHEMA_VERSION,
            "generated_at": summary.generated_at.isoformat(),
            "database_path": summary.database_path,
            "output_directory": summary.output_directory,
            "dataset": {
                "dataset_rows": summary.dataset_rows,
                "event_rows": summary.event_rows,
                "cycles": summary.cycles,
                "unique_assets": summary.unique_assets,
                "unique_tokens": summary.unique_tokens,
                "successful_quotes": summary.successful_quotes,
                "quote_errors": summary.quote_errors,
                "eligible_events": summary.eligible_events,
                "profitable_events": summary.profitable_events,
                "execution_candidates": summary.execution_candidates,
            },
            "validation": {
                "passed": summary.validation_passed,
                "errors": summary.validation_errors,
                "warnings": summary.validation_warnings,
                "checks_run": report.checks_run,
                "checks_passed": report.checks_passed,
                "checks_failed": report.checks_failed,
            },
            "files": [
                exported_file.to_dict()
                for exported_file in summary.files
            ],
        }

        def writer(temp_path: Path) -> None:
            temp_path.write_text(
                json.dumps(
                    manifest,
                    indent=2 if self.configuration.pretty_json else None,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

        self._atomic_write(path, writer)

        return self._describe_file(
            label="manifest",
            path=path,
            file_format="json",
            rows=None,
        )

    def _atomic_write(
        self,
        destination: Path,
        writer: Any,
    ) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)

        if destination.exists() and not self.configuration.overwrite:
            raise DatasetExportError(
                f"Refusing to overwrite existing file: {destination}"
            )

        file_descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{destination.name}.",
            suffix=".tmp",
            dir=str(destination.parent),
        )
        os.close(file_descriptor)
        temporary_path = Path(temporary_name)

        try:
            writer(temporary_path)

            if not temporary_path.exists():
                raise DatasetExportError(
                    f"Temporary export file was not created: {temporary_path}"
                )

            if temporary_path.stat().st_size == 0:
                raise DatasetExportError(
                    f"Temporary export file is empty: {temporary_path}"
                )

            os.replace(temporary_path, destination)

        except Exception:
            temporary_path.unlink(missing_ok=True)
            raise

    @staticmethod
    def _describe_file(
        *,
        label: str,
        path: Path,
        file_format: str,
        rows: int | None,
    ) -> ExportedFile:
        return ExportedFile(
            label=label,
            path=str(path),
            format=file_format,
            rows=rows,
            bytes=path.stat().st_size,
            sha256=_sha256_file(path),
        )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def verify_exported_file(file: ExportedFile) -> bool:
    path = Path(file.path)

    if not path.exists() or not path.is_file():
        return False

    if path.stat().st_size != file.bytes:
        return False

    return _sha256_file(path) == file.sha256


def verify_export_summary(
    summary: DatasetExportSummary,
) -> tuple[bool, tuple[str, ...]]:
    failures: list[str] = []

    for exported_file in summary.files:
        if not verify_exported_file(exported_file):
            failures.append(exported_file.path)

    return not failures, tuple(failures)


def export_historical_dataset(
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    *,
    output_directory: str | Path = DEFAULT_EXPORT_DIRECTORY,
    export_csv: bool = True,
    export_json: bool = True,
    export_jsonl: bool = True,
    export_cycle_csv: bool = True,
    export_validation_report: bool = True,
    export_manifest: bool = True,
    pretty_json: bool = True,
    overwrite: bool = True,
    allow_invalid: bool = False,
    strict_loading: bool = True,
) -> DatasetExportSummary:
    configuration = ExportConfiguration(
        output_directory=Path(output_directory),
        export_csv=export_csv,
        export_json=export_json,
        export_jsonl=export_jsonl,
        export_cycle_csv=export_cycle_csv,
        export_validation_report=export_validation_report,
        export_manifest=export_manifest,
        pretty_json=pretty_json,
        overwrite=overwrite,
        allow_invalid=allow_invalid,
        strict_loading=strict_loading,
    )

    exporter = DatasetExporter(configuration)
    return exporter.export_database(database_path)


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Validate and export the historical backtest research dataset."
        )
    )
    parser.add_argument(
        "--database",
        default=str(DEFAULT_DATABASE_PATH),
        help="Path to trades.db",
    )
    parser.add_argument(
        "--output-directory",
        default=str(DEFAULT_EXPORT_DIRECTORY),
        help="Directory for exported files",
    )
    parser.add_argument(
        "--csv-only",
        action="store_true",
        help="Export only event CSV plus manifest",
    )
    parser.add_argument(
        "--json-only",
        action="store_true",
        help="Export only event JSON plus manifest",
    )
    parser.add_argument(
        "--jsonl-only",
        action="store_true",
        help="Export only event JSONL plus manifest",
    )
    parser.add_argument(
        "--no-csv",
        action="store_true",
        help="Disable event CSV export",
    )
    parser.add_argument(
        "--no-json",
        action="store_true",
        help="Disable event JSON export",
    )
    parser.add_argument(
        "--no-jsonl",
        action="store_true",
        help="Disable event JSONL export",
    )
    parser.add_argument(
        "--no-cycle-csv",
        action="store_true",
        help="Disable cycle summary CSV export",
    )
    parser.add_argument(
        "--no-validation-report",
        action="store_true",
        help="Disable validation report export",
    )
    parser.add_argument(
        "--no-manifest",
        action="store_true",
        help="Disable manifest export",
    )
    parser.add_argument(
        "--compact-json",
        action="store_true",
        help="Write compact JSON instead of indented JSON",
    )
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        help="Refuse to overwrite existing export files",
    )
    parser.add_argument(
        "--allow-invalid",
        action="store_true",
        help="Allow diagnostic exports even if validation fails",
    )
    parser.add_argument(
        "--non-strict",
        action="store_true",
        help="Skip malformed historical source rows",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify exported file size and SHA-256 after writing",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser


def _resolve_format_flags(
    args: argparse.Namespace,
) -> Mapping[str, bool]:
    exclusive_flags = (
        args.csv_only,
        args.json_only,
        args.jsonl_only,
    )

    if sum(bool(flag) for flag in exclusive_flags) > 1:
        raise InvalidExportConfigurationError(
            "Use only one of --csv-only, --json-only, or --jsonl-only."
        )

    if args.csv_only:
        return {
            "export_csv": True,
            "export_json": False,
            "export_jsonl": False,
            "export_cycle_csv": False,
            "export_validation_report": False,
        }

    if args.json_only:
        return {
            "export_csv": False,
            "export_json": True,
            "export_jsonl": False,
            "export_cycle_csv": False,
            "export_validation_report": False,
        }

    if args.jsonl_only:
        return {
            "export_csv": False,
            "export_json": False,
            "export_jsonl": True,
            "export_cycle_csv": False,
            "export_validation_report": False,
        }

    return {
        "export_csv": not args.no_csv,
        "export_json": not args.no_json,
        "export_jsonl": not args.no_jsonl,
        "export_cycle_csv": not args.no_cycle_csv,
        "export_validation_report": not args.no_validation_report,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_argument_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.verbose)

    try:
        formats = _resolve_format_flags(args)

        summary = export_historical_dataset(
            database_path=args.database,
            output_directory=args.output_directory,
            export_csv=formats["export_csv"],
            export_json=formats["export_json"],
            export_jsonl=formats["export_jsonl"],
            export_cycle_csv=formats["export_cycle_csv"],
            export_validation_report=formats[
                "export_validation_report"
            ],
            export_manifest=not args.no_manifest,
            pretty_json=not args.compact_json,
            overwrite=not args.no_overwrite,
            allow_invalid=args.allow_invalid,
            strict_loading=not args.non_strict,
        )

    except (
        DatasetExportError,
        DatasetValidationError,
        HistoricalDatasetError,
        EventBuilderError,
        OSError,
        ValueError,
    ) as error:
        LOGGER.error("%s", error)
        return 1

    print("\nHistorical Backtest Dataset Export")
    print("=" * 76)
    print(f"Database: {summary.database_path}")
    print(f"Output directory: {summary.output_directory}")
    print(f"Schema version: {summary.schema_version}")
    print()

    print("Dataset Summary")
    print("-" * 76)
    print(f"Dataset rows: {summary.dataset_rows}")
    print(f"Backtest events: {summary.event_rows}")
    print(f"Cycles: {summary.cycles}")
    print(f"Unique assets: {summary.unique_assets}")
    print(f"Unique tokens: {summary.unique_tokens}")
    print(f"Successful quotes: {summary.successful_quotes}")
    print(f"Quote errors: {summary.quote_errors}")
    print(f"Eligible events: {summary.eligible_events}")
    print(f"Profitable events: {summary.profitable_events}")
    print(f"Execution candidates: {summary.execution_candidates}")
    print()

    print("Validation")
    print("-" * 76)
    print(f"Passed: {summary.validation_passed}")
    print(f"Errors: {summary.validation_errors}")
    print(f"Warnings: {summary.validation_warnings}")
    print()

    print("Exported Files")
    print("-" * 76)

    for exported_file in summary.files:
        row_text = (
            str(exported_file.rows)
            if exported_file.rows is not None
            else "-"
        )
        print(
            f"{exported_file.label:20} | "
            f"rows={row_text:>5} | "
            f"bytes={exported_file.bytes:>10} | "
            f"{exported_file.path}"
        )
        print(f"{'':20}   sha256={exported_file.sha256}")

    if args.verify:
        valid, failures = verify_export_summary(summary)
        print()
        print("Post-Export Verification")
        print("-" * 76)
        print(f"VALID: {valid}")

        if failures:
            for failed_path in failures:
                print(f"FAILED: {failed_path}")

        if not valid:
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())