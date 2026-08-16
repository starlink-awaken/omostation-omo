"""Legacy (L) dimension for 4P3V1L1H framework."""

from omo._vendored.omo_debt_engine.legacy.age import calculate_age_score
from omo._vendored.omo_debt_engine.legacy.core import adjust_score_with_legacy, calculate_legacy_score
from omo._vendored.omo_debt_engine.legacy.migration import calculate_migration_path_score
from omo._vendored.omo_debt_engine.legacy.resistance import calculate_refactoring_resistance_score

__all__ = [
    "adjust_score_with_legacy",
    "calculate_age_score",
    "calculate_legacy_score",
    "calculate_migration_path_score",
    "calculate_refactoring_resistance_score",
]
