"""
omo-debt honesty dimension module

Implements the Honesty (H) dimension for 4P3V1L1H framework integration.

Sub-dimensions:
- Completeness: Coverage of technical debt disclosure
- Consistency: Objectivity and accuracy of assessments
- Verifiability: Traceability and evidence support
"""

from omo._vendored.omo_debt_engine.honesty.completeness import calculate_completeness
from omo._vendored.omo_debt_engine.honesty.consistency import calculate_consistency
from omo._vendored.omo_debt_engine.honesty.core import calculate_honesty_score
from omo._vendored.omo_debt_engine.honesty.verifiability import calculate_verifiability

__all__ = [
    "calculate_completeness",
    "calculate_consistency",
    "calculate_honesty_score",
    "calculate_verifiability",
]
