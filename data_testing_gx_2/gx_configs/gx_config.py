from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from great_expectations.expectations.expectation import MetaExpectation


@dataclass(frozen=True)
class GXValidationConfig:
    """A data class containing all data to configure a GX validation flow.

    Attributes:
        data_source (str): The source of the data.
        data_asset (str): The specific data asset being validated.
        batch_definition (str): The definition of the data batch.
        expectation_suite (str): The name of the expectation suite.
        validation_definition (str): The definition of the validation process.
        checkpoint (str): The checkpoint configuration for validation.
        expectations (dict[MetaExpectation, dict[str, Any]]): A dictionary mapping
        meta-expectations to their configurations.

    """

    data_source: str
    data_asset: str
    batch_definition: str
    example_data: str
    expectation_suite: str
    validation_definition: str
    checkpoint: str
    expectations: dict[MetaExpectation, dict[str, Any]]
