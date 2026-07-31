"""
Phase 11D — Machine Learning Research Engine

Chronological, zero-lookahead research framework for binary profitability
classification using the Phase 11A feature store.

Goals
-----
- Train only on historical training cycles.
- Evaluate only on untouched holdout cycles.
- Keep realized labels separate from model features.
- Detect insufficient class balance and refuse unreliable training.
- Provide baseline logistic regression and optional scikit-learn models.
- Export metrics, predictions, feature importance, and a model registry entry.
- Never modify SQLite, the scanner, risk controls, or live execution.

Run from the project root:

    python3 -m research.ml_research_engine

Optional enhanced models require scikit-learn:

    pip install scikit-learn
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
import pickle
import random
import statistics
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol, Sequence

try:
    from research.feature_store import (
        FeatureRow,
        FeatureStoreConfiguration,
        FeatureStoreError,
        build_feature_store,
    )
    from backtesting.historical_dataset import DEFAULT_DATABASE_PATH
except ModuleNotFoundError:
    from feature_store import (  # type: ignore
        FeatureRow,
        FeatureStoreConfiguration,
        FeatureStoreError,
        build_feature_store,
    )
    from historical_dataset import DEFAULT_DATABASE_PATH  # type: ignore


LOGGER = logging.getLogger(__name__)

ML_RESEARCH_SCHEMA_VERSION = "11D.1.0"

DEFAULT_OUTPUT_DIRECTORY = Path("research") / "ml_research"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIRECTORY / "ml_research_report.json"
DEFAULT_PREDICTIONS_CSV = DEFAULT_OUTPUT_DIRECTORY / "holdout_predictions.csv"
DEFAULT_FEATURE_IMPORTANCE_CSV = (
    DEFAULT_OUTPUT_DIRECTORY / "feature_importance.csv"
)
DEFAULT_MODEL_REGISTRY_JSON = (
    DEFAULT_OUTPUT_DIRECTORY / "model_registry.json"
)
DEFAULT_MODEL_ARTIFACT = (
    DEFAULT_OUTPUT_DIRECTORY / "champion_model.pkl"
)


class MLResearchError(RuntimeError):
    """Base exception for machine-learning research failures."""


class InvalidMLConfigurationError(MLResearchError):
    """Raised when ML settings are invalid."""


class InsufficientMLDataError(MLResearchError):
    """Raised when there is not enough evidence to train safely."""


@dataclass(frozen=True, slots=True)
class MLResearchConfiguration:
    training_fraction: float = 0.70
    minimum_training_rows: int = 500
    minimum_holdout_rows: int = 200
    minimum_positive_training_examples: int = 30
    minimum_negative_training_examples: int = 100
    minimum_positive_holdout_examples: int = 10
    random_seed: int = 42
    probability_threshold: float = 0.50
    maximum_iterations: int = 1_500
    learning_rate: float = 0.05
    l2_penalty: float = 0.01
    enable_sklearn_models: bool = True
    output_directory: Path = DEFAULT_OUTPUT_DIRECTORY
    overwrite: bool = True

    def validate(self) -> None:
        if not 0.50 <= self.training_fraction < 1.0:
            raise InvalidMLConfigurationError(
                "training_fraction must be in [0.50, 1.0)."
            )

        integer_fields = (
            "minimum_training_rows",
            "minimum_holdout_rows",
            "minimum_positive_training_examples",
            "minimum_negative_training_examples",
            "minimum_positive_holdout_examples",
            "maximum_iterations",
        )

        for name in integer_fields:
            value = int(getattr(self, name))
            if value <= 0:
                raise InvalidMLConfigurationError(
                    f"{name} must be positive."
                )

        if not 0.0 < self.probability_threshold < 1.0:
            raise InvalidMLConfigurationError(
                "probability_threshold must be in (0, 1)."
            )

        if not 0.0 < self.learning_rate <= 1.0:
            raise InvalidMLConfigurationError(
                "learning_rate must be in (0, 1]."
            )

        if self.l2_penalty < 0.0:
            raise InvalidMLConfigurationError(
                "l2_penalty cannot be negative."
            )


@dataclass(frozen=True, slots=True)
class DatasetPartition:
    training_rows: tuple[FeatureRow, ...]
    holdout_rows: tuple[FeatureRow, ...]
    training_cycle_ids: tuple[str, ...]
    holdout_cycle_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ModelMetrics:
    rows: int
    positives: int
    negatives: int
    predicted_positives: int
    predicted_negatives: int

    true_positives: int
    true_negatives: int
    false_positives: int
    false_negatives: int

    accuracy: float
    balanced_accuracy: float
    precision: float
    recall: float
    specificity: float
    f1_score: float
    brier_score: float
    log_loss: float
    roc_auc: float | None
    average_precision: float | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ModelEvaluation:
    model_name: str
    model_version: str
    training_metrics: ModelMetrics
    holdout_metrics: ModelMetrics
    feature_importance: tuple[tuple[str, float], ...]
    artifact_path: str | None
    status: str
    rejection_reasons: tuple[str, ...]
    score: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_name": self.model_name,
            "model_version": self.model_version,
            "training_metrics": self.training_metrics.to_dict(),
            "holdout_metrics": self.holdout_metrics.to_dict(),
            "feature_importance": [
                {"feature": name, "importance": importance}
                for name, importance in self.feature_importance
            ],
            "artifact_path": self.artifact_path,
            "status": self.status,
            "rejection_reasons": list(self.rejection_reasons),
            "score": self.score,
        }


@dataclass(frozen=True, slots=True)
class PredictionRecord:
    source_event_id: int
    timestamp: datetime
    cycle_id: str
    token: str
    asset_key: str
    actual_profitable: bool
    predicted_profitable: bool
    probability: float
    model_name: str
    realized_net_profit_usd: float

    def to_record(self) -> dict[str, Any]:
        result = asdict(self)
        result["timestamp"] = self.timestamp.isoformat(sep=" ")
        return result


@dataclass(frozen=True, slots=True)
class MLResearchSummary:
    generated_at: datetime
    schema_version: str
    total_rows: int
    total_cycles: int
    training_rows: int
    training_cycles: int
    holdout_rows: int
    holdout_cycles: int
    training_positives: int
    holdout_positives: int
    models_attempted: int
    models_completed: int
    champion_model: str | None
    champion_status: str | None
    champion_holdout_brier: float | None
    champion_holdout_balanced_accuracy: float | None
    promotion_allowed: bool
    statistically_sufficient: bool
    insufficiency_reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["generated_at"] = self.generated_at.isoformat()
        return result


MODEL_FEATURES: tuple[str, ...] = (
    "hour_utc",
    "weekday_utc",
    "cycle_position",
    "cycle_size",
    "cycle_progress",
    "starting_amount_usd",
    "estimated_cost_usd",
    "cost_bps",
    "source_decision_rank",
    "source_eligible",
    "quote_successful",
    "market_score",
    "liquidity_score",
    "volume_score",
    "pair_score",
    "intelligence_score",
    "composite_market_score",
    "score_dispersion",
    "minimum_component_score",
    "maximum_component_score",
    "score_range",
    "has_mint",
    "has_route",
    "has_error",
    "prior_token_observations",
    "prior_token_profitable_observations",
    "prior_token_win_rate",
    "prior_token_average_net_profit_usd",
    "prior_token_average_cost_bps",
    "prior_global_observations",
    "prior_global_profitable_observations",
    "prior_global_win_rate",
    "prior_global_average_net_profit_usd",
    "rolling_token_observations",
    "rolling_token_win_rate",
    "rolling_token_average_profit_usd",
    "rolling_token_profit_std_usd",
    "rolling_token_average_composite_score",
    "rolling_global_observations",
    "rolling_global_win_rate",
    "rolling_global_average_profit_usd",
    "rolling_global_profit_std_usd",
    "rolling_global_average_composite_score",
    "prior_cycle_average_market_score",
    "prior_cycle_average_composite_score",
    "prior_cycle_profitable_rate",
    "prior_cycle_average_net_profit_usd",
    "cycles_since_token_seen",
)


class ResearchModel(Protocol):
    name: str
    version: str

    def fit(
        self,
        features: Sequence[Sequence[float]],
        labels: Sequence[int],
    ) -> None:
        ...

    def predict_proba(
        self,
        features: Sequence[Sequence[float]],
    ) -> list[float]:
        ...

    def feature_importance(
        self,
        feature_names: Sequence[str],
    ) -> tuple[tuple[str, float], ...]:
        ...


@dataclass(slots=True)
class StandardScaler:
    means: list[float]
    standard_deviations: list[float]

    @classmethod
    def fit(
        cls,
        rows: Sequence[Sequence[float]],
    ) -> "StandardScaler":
        if not rows:
            raise MLResearchError(
                "Cannot fit scaler on an empty matrix."
            )

        columns = list(zip(*rows))
        means = [
            statistics.fmean(column)
            for column in columns
        ]

        deviations = [
            statistics.pstdev(column)
            if len(column) > 1
            else 0.0
            for column in columns
        ]

        deviations = [
            deviation if deviation > 1e-12 else 1.0
            for deviation in deviations
        ]

        return cls(
            means=means,
            standard_deviations=deviations,
        )

    def transform(
        self,
        rows: Sequence[Sequence[float]],
    ) -> list[list[float]]:
        return [
            [
                (float(value) - mean) / deviation
                for value, mean, deviation in zip(
                    row,
                    self.means,
                    self.standard_deviations,
                )
            ]
            for row in rows
        ]


class NativeLogisticRegression:
    """Dependency-free logistic-regression baseline."""

    name = "NativeLogisticRegression"
    version = "1.0"

    def __init__(
        self,
        *,
        learning_rate: float,
        maximum_iterations: int,
        l2_penalty: float,
    ) -> None:
        self.learning_rate = learning_rate
        self.maximum_iterations = maximum_iterations
        self.l2_penalty = l2_penalty
        self.weights: list[float] = []
        self.bias = 0.0

    def fit(
        self,
        features: Sequence[Sequence[float]],
        labels: Sequence[int],
    ) -> None:
        if not features:
            raise MLResearchError(
                "Cannot train on an empty feature matrix."
            )

        if len(features) != len(labels):
            raise MLResearchError(
                "Feature and label counts do not match."
            )

        feature_count = len(features[0])
        self.weights = [0.0] * feature_count
        self.bias = 0.0

        positive_count = sum(labels)
        negative_count = len(labels) - positive_count

        if positive_count == 0 or negative_count == 0:
            raise InsufficientMLDataError(
                "Both classes are required for logistic regression."
            )

        positive_weight = len(labels) / (2.0 * positive_count)
        negative_weight = len(labels) / (2.0 * negative_count)

        for _iteration in range(self.maximum_iterations):
            gradient_weights = [0.0] * feature_count
            gradient_bias = 0.0

            for row, label in zip(features, labels):
                linear = self.bias + sum(
                    weight * value
                    for weight, value in zip(
                        self.weights,
                        row,
                    )
                )

                probability = _sigmoid(linear)
                sample_weight = (
                    positive_weight
                    if label == 1
                    else negative_weight
                )
                error = (
                    probability - label
                ) * sample_weight

                for index, value in enumerate(row):
                    gradient_weights[index] += error * value

                gradient_bias += error

            count = float(len(labels))

            for index in range(feature_count):
                regularization = (
                    self.l2_penalty
                    * self.weights[index]
                )

                self.weights[index] -= self.learning_rate * (
                    gradient_weights[index] / count
                    + regularization
                )

            self.bias -= (
                self.learning_rate
                * gradient_bias
                / count
            )

    def predict_proba(
        self,
        features: Sequence[Sequence[float]],
    ) -> list[float]:
        if not self.weights:
            raise MLResearchError(
                "Model has not been trained."
            )

        return [
            _sigmoid(
                self.bias
                + sum(
                    weight * value
                    for weight, value in zip(
                        self.weights,
                        row,
                    )
                )
            )
            for row in features
        ]

    def feature_importance(
        self,
        feature_names: Sequence[str],
    ) -> tuple[tuple[str, float], ...]:
        pairs = [
            (name, abs(weight))
            for name, weight in zip(
                feature_names,
                self.weights,
            )
        ]

        pairs.sort(
            key=lambda item: item[1],
            reverse=True,
        )

        total = sum(value for _, value in pairs)

        if total > 0:
            pairs = [
                (name, value / total)
                for name, value in pairs
            ]

        return tuple(pairs)


class SklearnModelAdapter:
    def __init__(
        self,
        estimator: Any,
        *,
        name: str,
        version: str,
    ) -> None:
        self.estimator = estimator
        self.name = name
        self.version = version

    def fit(
        self,
        features: Sequence[Sequence[float]],
        labels: Sequence[int],
    ) -> None:
        self.estimator.fit(features, labels)

    def predict_proba(
        self,
        features: Sequence[Sequence[float]],
    ) -> list[float]:
        probabilities = self.estimator.predict_proba(
            features
        )

        return [
            float(row[1])
            for row in probabilities
        ]

    def feature_importance(
        self,
        feature_names: Sequence[str],
    ) -> tuple[tuple[str, float], ...]:
        if hasattr(self.estimator, "feature_importances_"):
            raw = list(
                self.estimator.feature_importances_
            )
        elif hasattr(self.estimator, "coef_"):
            raw = [
                abs(float(value))
                for value in self.estimator.coef_[0]
            ]
        else:
            return ()

        pairs = list(
            zip(feature_names, raw)
        )
        pairs.sort(
            key=lambda item: item[1],
            reverse=True,
        )

        total = sum(value for _, value in pairs)

        if total > 0:
            pairs = [
                (name, value / total)
                for name, value in pairs
            ]

        return tuple(
            (str(name), float(value))
            for name, value in pairs
        )


class MLResearchEngine:
    def __init__(
        self,
        configuration: MLResearchConfiguration | None = None,
    ) -> None:
        self.configuration = (
            configuration
            or MLResearchConfiguration()
        )
        self.configuration.validate()

    def run(
        self,
        rows: Sequence[FeatureRow],
    ) -> tuple[
        tuple[ModelEvaluation, ...],
        tuple[PredictionRecord, ...],
        MLResearchSummary,
    ]:
        labeled_rows = tuple(
            row
            for row in rows
            if row.quote_successful
            and row.profitable_label is not None
            and row.realized_net_profit_usd is not None
        )

        if not labeled_rows:
            raise InsufficientMLDataError(
                "No labeled successful quotes are available."
            )

        partition = self._partition(
            labeled_rows
        )

        sufficiency_reasons = self._sufficiency_reasons(
            partition
        )

        statistically_sufficient = (
            not sufficiency_reasons
        )

        if not statistically_sufficient:
            summary = MLResearchSummary(
                generated_at=datetime.now(timezone.utc),
                schema_version=ML_RESEARCH_SCHEMA_VERSION,
                total_rows=len(labeled_rows),
                total_cycles=len(
                    {row.cycle_id for row in labeled_rows}
                ),
                training_rows=len(
                    partition.training_rows
                ),
                training_cycles=len(
                    partition.training_cycle_ids
                ),
                holdout_rows=len(
                    partition.holdout_rows
                ),
                holdout_cycles=len(
                    partition.holdout_cycle_ids
                ),
                training_positives=sum(
                    row.profitable_label is True
                    for row in partition.training_rows
                ),
                holdout_positives=sum(
                    row.profitable_label is True
                    for row in partition.holdout_rows
                ),
                models_attempted=0,
                models_completed=0,
                champion_model=None,
                champion_status="BLOCKED_INSUFFICIENT_DATA",
                champion_holdout_brier=None,
                champion_holdout_balanced_accuracy=None,
                promotion_allowed=False,
                statistically_sufficient=False,
                insufficiency_reasons=tuple(
                    sufficiency_reasons
                ),
            )

            return (), (), summary

        training_matrix = _matrix_from_rows(
            partition.training_rows
        )
        holdout_matrix = _matrix_from_rows(
            partition.holdout_rows
        )

        training_labels = _labels_from_rows(
            partition.training_rows
        )
        holdout_labels = _labels_from_rows(
            partition.holdout_rows
        )

        scaler = StandardScaler.fit(
            training_matrix
        )
        scaled_training = scaler.transform(
            training_matrix
        )
        scaled_holdout = scaler.transform(
            holdout_matrix
        )

        models = self._models()
        evaluations: list[ModelEvaluation] = []
        prediction_records_by_model: dict[
            str,
            tuple[PredictionRecord, ...],
        ] = {}

        output = self.configuration.output_directory
        output.mkdir(
            parents=True,
            exist_ok=True,
        )

        for model in models:
            try:
                model.fit(
                    scaled_training,
                    training_labels,
                )

                training_probabilities = (
                    model.predict_proba(
                        scaled_training
                    )
                )
                holdout_probabilities = (
                    model.predict_proba(
                        scaled_holdout
                    )
                )

                training_metrics = calculate_metrics(
                    training_labels,
                    training_probabilities,
                    threshold=(
                        self.configuration
                        .probability_threshold
                    ),
                )

                holdout_metrics = calculate_metrics(
                    holdout_labels,
                    holdout_probabilities,
                    threshold=(
                        self.configuration
                        .probability_threshold
                    ),
                )

                rejection_reasons = (
                    self._model_rejection_reasons(
                        training_metrics,
                        holdout_metrics,
                    )
                )

                status = (
                    "RESEARCH_CANDIDATE"
                    if not rejection_reasons
                    else "REJECTED"
                )

                score = self._model_score(
                    training_metrics,
                    holdout_metrics,
                    rejection_reasons,
                )

                artifact_path = output / (
                    model.name
                    + "_"
                    + model.version
                    + ".pkl"
                )

                with artifact_path.open(
                    "wb"
                ) as handle:
                    pickle.dump(
                        {
                            "schema_version": (
                                ML_RESEARCH_SCHEMA_VERSION
                            ),
                            "model_name": model.name,
                            "model_version": model.version,
                            "model": model,
                            "scaler": scaler,
                            "feature_names": MODEL_FEATURES,
                            "probability_threshold": (
                                self.configuration
                                .probability_threshold
                            ),
                        },
                        handle,
                    )

                evaluation = ModelEvaluation(
                    model_name=model.name,
                    model_version=model.version,
                    training_metrics=training_metrics,
                    holdout_metrics=holdout_metrics,
                    feature_importance=(
                        model.feature_importance(
                            MODEL_FEATURES
                        )
                    ),
                    artifact_path=str(
                        artifact_path
                    ),
                    status=status,
                    rejection_reasons=tuple(
                        rejection_reasons
                    ),
                    score=score,
                )

                evaluations.append(
                    evaluation
                )

                prediction_records_by_model[
                    model.name
                ] = tuple(
                    PredictionRecord(
                        source_event_id=row.source_event_id,
                        timestamp=row.timestamp,
                        cycle_id=row.cycle_id,
                        token=row.token,
                        asset_key=row.asset_key,
                        actual_profitable=bool(
                            row.profitable_label
                        ),
                        predicted_profitable=(
                            probability
                            >= self.configuration
                            .probability_threshold
                        ),
                        probability=probability,
                        model_name=model.name,
                        realized_net_profit_usd=float(
                            row.realized_net_profit_usd
                            or 0.0
                        ),
                    )
                    for row, probability in zip(
                        partition.holdout_rows,
                        holdout_probabilities,
                    )
                )

            except Exception as error:
                LOGGER.exception(
                    "Model %s failed: %s",
                    model.name,
                    error,
                )

        evaluations.sort(
            key=lambda item: item.score,
            reverse=True,
        )

        champion = (
            evaluations[0]
            if evaluations
            else None
        )

        predictions = (
            prediction_records_by_model.get(
                champion.model_name,
                (),
            )
            if champion
            else ()
        )

        promotion_allowed = bool(
            champion
            and champion.status
            == "RESEARCH_CANDIDATE"
        )

        summary = MLResearchSummary(
            generated_at=datetime.now(timezone.utc),
            schema_version=ML_RESEARCH_SCHEMA_VERSION,
            total_rows=len(labeled_rows),
            total_cycles=len(
                {row.cycle_id for row in labeled_rows}
            ),
            training_rows=len(
                partition.training_rows
            ),
            training_cycles=len(
                partition.training_cycle_ids
            ),
            holdout_rows=len(
                partition.holdout_rows
            ),
            holdout_cycles=len(
                partition.holdout_cycle_ids
            ),
            training_positives=sum(
                training_labels
            ),
            holdout_positives=sum(
                holdout_labels
            ),
            models_attempted=len(models),
            models_completed=len(
                evaluations
            ),
            champion_model=(
                champion.model_name
                if champion
                else None
            ),
            champion_status=(
                champion.status
                if champion
                else None
            ),
            champion_holdout_brier=(
                champion.holdout_metrics.brier_score
                if champion
                else None
            ),
            champion_holdout_balanced_accuracy=(
                champion
                .holdout_metrics
                .balanced_accuracy
                if champion
                else None
            ),
            promotion_allowed=promotion_allowed,
            statistically_sufficient=True,
            insufficiency_reasons=(),
        )

        return (
            tuple(evaluations),
            tuple(predictions),
            summary,
        )

    def _partition(
        self,
        rows: Sequence[FeatureRow],
    ) -> DatasetPartition:
        cycle_ids: list[str] = []
        seen: set[str] = set()

        for row in rows:
            if row.cycle_id not in seen:
                seen.add(row.cycle_id)
                cycle_ids.append(row.cycle_id)

        if len(cycle_ids) < 2:
            raise InsufficientMLDataError(
                "At least two chronological cycles are required."
            )

        training_cycle_count = max(
            1,
            int(
                math.floor(
                    len(cycle_ids)
                    * self.configuration
                    .training_fraction
                )
            ),
        )

        training_cycle_count = min(
            training_cycle_count,
            len(cycle_ids) - 1,
        )

        training_cycle_ids = tuple(
            cycle_ids[:training_cycle_count]
        )
        holdout_cycle_ids = tuple(
            cycle_ids[training_cycle_count:]
        )

        training_set = set(
            training_cycle_ids
        )

        training_rows = tuple(
            row
            for row in rows
            if row.cycle_id in training_set
        )

        holdout_rows = tuple(
            row
            for row in rows
            if row.cycle_id not in training_set
        )

        return DatasetPartition(
            training_rows=training_rows,
            holdout_rows=holdout_rows,
            training_cycle_ids=training_cycle_ids,
            holdout_cycle_ids=holdout_cycle_ids,
        )

    def _sufficiency_reasons(
        self,
        partition: DatasetPartition,
    ) -> list[str]:
        reasons: list[str] = []

        training_positive = sum(
            row.profitable_label is True
            for row in partition.training_rows
        )
        training_negative = sum(
            row.profitable_label is False
            for row in partition.training_rows
        )
        holdout_positive = sum(
            row.profitable_label is True
            for row in partition.holdout_rows
        )

        if (
            len(partition.training_rows)
            < self.configuration.minimum_training_rows
        ):
            reasons.append(
                "Training rows are below the minimum."
            )

        if (
            len(partition.holdout_rows)
            < self.configuration.minimum_holdout_rows
        ):
            reasons.append(
                "Holdout rows are below the minimum."
            )

        if (
            training_positive
            < self.configuration
            .minimum_positive_training_examples
        ):
            reasons.append(
                "Too few positive training examples."
            )

        if (
            training_negative
            < self.configuration
            .minimum_negative_training_examples
        ):
            reasons.append(
                "Too few negative training examples."
            )

        if (
            holdout_positive
            < self.configuration
            .minimum_positive_holdout_examples
        ):
            reasons.append(
                "Too few positive holdout examples."
            )

        if len(partition.training_cycle_ids) < 20:
            reasons.append(
                "Fewer than 20 training cycles."
            )

        if len(partition.holdout_cycle_ids) < 5:
            reasons.append(
                "Fewer than 5 holdout cycles."
            )

        return reasons

    def _models(
        self,
    ) -> tuple[ResearchModel, ...]:
        models: list[ResearchModel] = [
            NativeLogisticRegression(
                learning_rate=(
                    self.configuration.learning_rate
                ),
                maximum_iterations=(
                    self.configuration
                    .maximum_iterations
                ),
                l2_penalty=(
                    self.configuration.l2_penalty
                ),
            )
        ]

        if not self.configuration.enable_sklearn_models:
            return tuple(models)

        try:
            from sklearn.ensemble import (
                GradientBoostingClassifier,
                RandomForestClassifier,
            )
            from sklearn.linear_model import LogisticRegression

            models.extend(
                (
                    SklearnModelAdapter(
                        LogisticRegression(
                            class_weight="balanced",
                            max_iter=2_000,
                            random_state=(
                                self.configuration.random_seed
                            ),
                        ),
                        name="SklearnLogisticRegression",
                        version="1.0",
                    ),
                    SklearnModelAdapter(
                        RandomForestClassifier(
                            n_estimators=300,
                            max_depth=6,
                            min_samples_leaf=10,
                            class_weight="balanced_subsample",
                            random_state=(
                                self.configuration.random_seed
                            ),
                            n_jobs=-1,
                        ),
                        name="RandomForestClassifier",
                        version="1.0",
                    ),
                    SklearnModelAdapter(
                        GradientBoostingClassifier(
                            n_estimators=150,
                            learning_rate=0.03,
                            max_depth=3,
                            min_samples_leaf=10,
                            random_state=(
                                self.configuration.random_seed
                            ),
                        ),
                        name="GradientBoostingClassifier",
                        version="1.0",
                    ),
                )
            )

        except ImportError:
            LOGGER.warning(
                "scikit-learn is not installed; "
                "running native logistic regression only."
            )

        return tuple(models)

    @staticmethod
    def _model_rejection_reasons(
        training: ModelMetrics,
        holdout: ModelMetrics,
    ) -> list[str]:
        reasons: list[str] = []

        if holdout.positives < 10:
            reasons.append(
                "Holdout contains fewer than 10 positive examples."
            )

        if holdout.balanced_accuracy < 0.55:
            reasons.append(
                "Holdout balanced accuracy is below 0.55."
            )

        if holdout.precision < 0.05:
            reasons.append(
                "Holdout precision is below 5%."
            )

        if holdout.recall < 0.10:
            reasons.append(
                "Holdout recall is below 10%."
            )

        if holdout.brier_score >= 0.25:
            reasons.append(
                "Holdout Brier score is not better than a weak baseline."
            )

        if (
            training.balanced_accuracy
            - holdout.balanced_accuracy
            > 0.20
        ):
            reasons.append(
                "Training-to-holdout performance gap indicates overfitting."
            )

        if holdout.predicted_positives == 0:
            reasons.append(
                "Model predicts no positive opportunities."
            )

        return reasons

    @staticmethod
    def _model_score(
        training: ModelMetrics,
        holdout: ModelMetrics,
        rejection_reasons: Sequence[str],
    ) -> float:
        return (
            holdout.balanced_accuracy * 4.0
            + holdout.precision * 2.0
            + holdout.recall * 2.0
            + (
                holdout.roc_auc
                if holdout.roc_auc is not None
                else 0.0
            )
            + (
                holdout.average_precision
                if holdout.average_precision is not None
                else 0.0
            )
            - holdout.brier_score * 2.0
            - max(
                0.0,
                training.balanced_accuracy
                - holdout.balanced_accuracy,
            )
            - len(rejection_reasons) * 2.0
        )


def _matrix_from_rows(
    rows: Sequence[FeatureRow],
) -> list[list[float]]:
    matrix: list[list[float]] = []

    for row in rows:
        values: list[float] = []

        for feature_name in MODEL_FEATURES:
            value = getattr(
                row,
                feature_name,
            )

            if isinstance(value, bool):
                numeric = float(value)
            elif value is None:
                numeric = 0.0
            else:
                numeric = float(value)

            if not math.isfinite(numeric):
                numeric = 0.0

            values.append(numeric)

        matrix.append(values)

    return matrix


def _labels_from_rows(
    rows: Sequence[FeatureRow],
) -> list[int]:
    return [
        1 if row.profitable_label else 0
        for row in rows
    ]


def calculate_metrics(
    labels: Sequence[int],
    probabilities: Sequence[float],
    *,
    threshold: float,
) -> ModelMetrics:
    if len(labels) != len(probabilities):
        raise MLResearchError(
            "Label and probability counts do not match."
        )

    predictions = [
        int(probability >= threshold)
        for probability in probabilities
    ]

    true_positives = sum(
        prediction == 1 and label == 1
        for prediction, label in zip(
            predictions,
            labels,
        )
    )
    true_negatives = sum(
        prediction == 0 and label == 0
        for prediction, label in zip(
            predictions,
            labels,
        )
    )
    false_positives = sum(
        prediction == 1 and label == 0
        for prediction, label in zip(
            predictions,
            labels,
        )
    )
    false_negatives = sum(
        prediction == 0 and label == 1
        for prediction, label in zip(
            predictions,
            labels,
        )
    )

    positives = sum(labels)
    negatives = len(labels) - positives

    precision = _safe_divide(
        true_positives,
        true_positives + false_positives,
    )
    recall = _safe_divide(
        true_positives,
        true_positives + false_negatives,
    )
    specificity = _safe_divide(
        true_negatives,
        true_negatives + false_positives,
    )

    f1_score = _safe_divide(
        2.0 * precision * recall,
        precision + recall,
    )

    clipped = [
        min(
            1.0 - 1e-15,
            max(1e-15, probability),
        )
        for probability in probabilities
    ]

    brier_score = (
        statistics.fmean(
            (
                probability - label
            )
            ** 2
            for probability, label in zip(
                clipped,
                labels,
            )
        )
        if labels
        else 0.0
    )

    log_loss = (
        -statistics.fmean(
            label * math.log(probability)
            + (1 - label)
            * math.log(1.0 - probability)
            for probability, label in zip(
                clipped,
                labels,
            )
        )
        if labels
        else 0.0
    )

    roc_auc = _roc_auc(
        labels,
        clipped,
    )
    average_precision = _average_precision(
        labels,
        clipped,
    )

    return ModelMetrics(
        rows=len(labels),
        positives=positives,
        negatives=negatives,
        predicted_positives=sum(predictions),
        predicted_negatives=(
            len(predictions) - sum(predictions)
        ),
        true_positives=true_positives,
        true_negatives=true_negatives,
        false_positives=false_positives,
        false_negatives=false_negatives,
        accuracy=_safe_divide(
            true_positives + true_negatives,
            len(labels),
        ),
        balanced_accuracy=(
            recall + specificity
        ) / 2.0,
        precision=precision,
        recall=recall,
        specificity=specificity,
        f1_score=f1_score,
        brier_score=brier_score,
        log_loss=log_loss,
        roc_auc=roc_auc,
        average_precision=average_precision,
    )


def _roc_auc(
    labels: Sequence[int],
    probabilities: Sequence[float],
) -> float | None:
    positives = sum(labels)
    negatives = len(labels) - positives

    if positives == 0 or negatives == 0:
        return None

    ranked = sorted(
        zip(probabilities, labels),
        key=lambda item: item[0],
    )

    rank_sum = 0.0
    index = 0
    rank = 1

    while index < len(ranked):
        end = index + 1

        while (
            end < len(ranked)
            and ranked[end][0]
            == ranked[index][0]
        ):
            end += 1

        average_rank = (
            rank + rank + (end - index) - 1
        ) / 2.0

        positive_in_group = sum(
            label
            for _, label in ranked[index:end]
        )

        rank_sum += (
            average_rank
            * positive_in_group
        )

        rank += end - index
        index = end

    return (
        rank_sum
        - positives
        * (positives + 1)
        / 2.0
    ) / (
        positives * negatives
    )


def _average_precision(
    labels: Sequence[int],
    probabilities: Sequence[float],
) -> float | None:
    positives = sum(labels)

    if positives == 0:
        return None

    ranked = sorted(
        zip(probabilities, labels),
        key=lambda item: item[0],
        reverse=True,
    )

    true_positives = 0
    precision_sum = 0.0

    for index, (_probability, label) in enumerate(
        ranked,
        start=1,
    ):
        if label == 1:
            true_positives += 1
            precision_sum += (
                true_positives / index
            )

    return precision_sum / positives


def _safe_divide(
    numerator: float,
    denominator: float,
) -> float:
    if denominator == 0:
        return 0.0

    return numerator / denominator


def _sigmoid(value: float) -> float:
    if value >= 0:
        exponential = math.exp(-value)
        return 1.0 / (1.0 + exponential)

    exponential = math.exp(value)
    return exponential / (1.0 + exponential)


def export_results(
    evaluations: Sequence[ModelEvaluation],
    predictions: Sequence[PredictionRecord],
    summary: MLResearchSummary,
    configuration: MLResearchConfiguration,
) -> None:
    output = configuration.output_directory
    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    report_path = output / DEFAULT_REPORT_JSON.name
    predictions_path = output / DEFAULT_PREDICTIONS_CSV.name
    importance_path = output / DEFAULT_FEATURE_IMPORTANCE_CSV.name
    registry_path = output / DEFAULT_MODEL_REGISTRY_JSON.name

    _ensure_writable(
        (
            report_path,
            predictions_path,
            importance_path,
            registry_path,
        ),
        overwrite=configuration.overwrite,
    )

    report_path.write_text(
        json.dumps(
            {
                "summary": summary.to_dict(),
                "configuration": {
                    **asdict(configuration),
                    "output_directory": str(
                        configuration.output_directory
                    ),
                },
                "models": [
                    evaluation.to_dict()
                    for evaluation in evaluations
                ],
                "warning": (
                    "Research models must not be connected to live execution "
                    "until the data-sufficiency, walk-forward, Monte Carlo, "
                    "and risk gates all pass."
                ),
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    _write_csv(
        predictions_path,
        [
            prediction.to_record()
            for prediction in predictions
        ],
    )

    importance_records: list[
        dict[str, Any]
    ] = []

    for evaluation in evaluations:
        for rank, (
            feature,
            importance,
        ) in enumerate(
            evaluation.feature_importance,
            start=1,
        ):
            importance_records.append(
                {
                    "model_name": (
                        evaluation.model_name
                    ),
                    "model_version": (
                        evaluation.model_version
                    ),
                    "rank": rank,
                    "feature": feature,
                    "importance": importance,
                }
            )

    _write_csv(
        importance_path,
        importance_records,
    )

    registry_path.write_text(
        json.dumps(
            {
                "schema_version": (
                    ML_RESEARCH_SCHEMA_VERSION
                ),
                "updated_at": (
                    datetime.now(timezone.utc)
                    .isoformat()
                ),
                "champion": (
                    evaluations[0].to_dict()
                    if evaluations
                    else None
                ),
                "models": [
                    evaluation.to_dict()
                    for evaluation in evaluations
                ],
                "promotion_allowed": (
                    summary.promotion_allowed
                ),
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def run_ml_research(
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    *,
    configuration: MLResearchConfiguration | None = None,
) -> MLResearchSummary:
    active_configuration = (
        configuration
        or MLResearchConfiguration()
    )
    active_configuration.validate()

    rows, _feature_summary = build_feature_store(
        database_path,
        configuration=FeatureStoreConfiguration(
            include_quote_errors=True,
            include_outcome_labels=True,
        ),
    )

    (
        evaluations,
        predictions,
        summary,
    ) = MLResearchEngine(
        active_configuration
    ).run(rows)

    export_results(
        evaluations,
        predictions,
        summary,
        active_configuration,
    )

    return summary


def _ensure_writable(
    paths: Sequence[Path],
    *,
    overwrite: bool,
) -> None:
    if overwrite:
        return

    existing = [
        path
        for path in paths
        if path.exists()
    ]

    if existing:
        raise MLResearchError(
            "Refusing to overwrite: "
            + ", ".join(
                str(path)
                for path in existing
            )
        )


def _write_csv(
    path: Path,
    records: Sequence[Mapping[str, Any]],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    if not records:
        path.write_text(
            "",
            encoding="utf-8",
        )
        return

    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(
                records[0].keys()
            ),
            extrasaction="raise",
        )
        writer.writeheader()
        writer.writerows(records)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run Phase 11D machine-learning research."
        )
    )

    parser.add_argument(
        "--database",
        default=str(DEFAULT_DATABASE_PATH),
    )

    parser.add_argument(
        "--output-directory",
        default=str(DEFAULT_OUTPUT_DIRECTORY),
    )

    parser.add_argument(
        "--training-fraction",
        type=float,
        default=0.70,
    )

    parser.add_argument(
        "--disable-sklearn",
        action="store_true",
    )

    parser.add_argument(
        "--no-overwrite",
        action="store_true",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
    )

    return parser


def main(
    argv: Sequence[str] | None = None,
) -> int:
    args = _parser().parse_args(
        argv
    )

    logging.basicConfig(
        level=(
            logging.DEBUG
            if args.verbose
            else logging.INFO
        ),
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(name)s | "
            "%(message)s"
        ),
    )

    configuration = MLResearchConfiguration(
        training_fraction=(
            args.training_fraction
        ),
        enable_sklearn_models=(
            not args.disable_sklearn
        ),
        output_directory=Path(
            args.output_directory
        ),
        overwrite=(
            not args.no_overwrite
        ),
    )

    try:
        summary = run_ml_research(
            args.database,
            configuration=configuration,
        )

    except (
        MLResearchError,
        FeatureStoreError,
        OSError,
        ValueError,
    ) as error:
        LOGGER.error(
            "%s",
            error,
        )
        return 1

    print(
        "\nPhase 11D — "
        "Machine Learning Research Engine"
    )
    print("=" * 80)
    print(f"Rows: {summary.total_rows}")
    print(f"Cycles: {summary.total_cycles}")
    print(
        "Training / holdout rows: "
        f"{summary.training_rows} / "
        f"{summary.holdout_rows}"
    )
    print(
        "Training / holdout cycles: "
        f"{summary.training_cycles} / "
        f"{summary.holdout_cycles}"
    )
    print(
        "Training / holdout positives: "
        f"{summary.training_positives} / "
        f"{summary.holdout_positives}"
    )
    print()

    print("Research status")
    print("-" * 80)
    print(
        "Statistically sufficient: "
        f"{summary.statistically_sufficient}"
    )
    print(
        "Models attempted / completed: "
        f"{summary.models_attempted} / "
        f"{summary.models_completed}"
    )
    print(
        f"Champion model: {summary.champion_model}"
    )
    print(
        f"Champion status: {summary.champion_status}"
    )
    print(
        "Promotion allowed: "
        f"{summary.promotion_allowed}"
    )

    if summary.insufficiency_reasons:
        print()
        print("Insufficiency reasons:")

        for reason in summary.insufficiency_reasons:
            print(f"  - {reason}")

    if summary.champion_holdout_brier is not None:
        print()
        print(
            "Champion holdout Brier score: "
            f"{summary.champion_holdout_brier:.6f}"
        )
        print(
            "Champion holdout balanced accuracy: "
            f"{summary.champion_holdout_balanced_accuracy:.6f}"
        )

    print()
    print("Output files")
    print("-" * 80)

    output = configuration.output_directory

    print(output / DEFAULT_REPORT_JSON.name)
    print(output / DEFAULT_PREDICTIONS_CSV.name)
    print(output / DEFAULT_FEATURE_IMPORTANCE_CSV.name)
    print(output / DEFAULT_MODEL_REGISTRY_JSON.name)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())