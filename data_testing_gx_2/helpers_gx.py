"""Utility and helper functions using Great Expectations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from great_expectations import expectations as gxe
from great_expectations.core.expectation_suite import ExpectationSuite

if TYPE_CHECKING:
    from great_expectations.checkpoint.checkpoint import Checkpoint
    from great_expectations.core.expectation_suite import ExpectationSuite
    from great_expectations.core.factory.checkpoint_factory import CheckpointFactory
    from great_expectations.core.factory.suite_factory import SuiteFactory
    from great_expectations.core.factory.validation_definition_factory import (
        ValidationDefinitionFactory,
    )
    from great_expectations.core.validation_definition import ValidationDefinition
    from great_expectations.expectations.expectation import MetaExpectation


def add_or_update_gx_artifact(
    artifact_factory: ValidationDefinitionFactory | SuiteFactory | CheckpointFactory,
    artifact: ValidationDefinition | ExpectationSuite | Checkpoint,
    *,
    replace_if_exists: bool = True,
) -> ValidationDefinition | ExpectationSuite | Checkpoint:
    """Add or recreate a Great Expectations artifact in the provided context.

    Parameters
    ----------
    artifact_factory : ValidationDefinitionFactory | SuiteFactory | CheckpointFactory
        The factory to be used to add or recreate the artifact.
    artifact : ValidationDefinition | ExpectationSuite | Checkpoint
        The artifact to be added or recreated.
    replace_if_exists : bool, optional
        If True, the existing artifact with the same name will be replaced
        if it exists. If False, an error will be raised if the artifact already exists.
        Default is True.

    Returns
    -------
    ValidationDefinition | ExpectationSuite | Checkpoint
        The artifact that was added or recreated.

    """
    artifact_name = artifact.name

    if replace_if_exists:
        current_artifacts = artifact_factory.all()
        current_artifact_names = [artifact.name for artifact in current_artifacts]

        if artifact_name in current_artifact_names:
            artifact_factory.delete(artifact_name)

    return artifact_factory.add(artifact)


def check_dict_for_keys(dict_in: dict, keys: list) -> bool:
    """Check if all specified keys are present in the dictionary.

    Parameters
    ----------
    dict_in : dict
        The dictionary to check.
    keys : list
        A list of keys to check for in the dictionary.

    Returns
    -------
    bool
        True if all keys are present in the dictionary, False otherwise.

    """
    return all(key in dict_in for key in keys)


def validate_suite_dictionary(dict_suite: dict) -> None:
    """Validate the structure and content of a suite dictionary.

    Parameters
    ----------
    dict_suite : dict
        A dictionary where keys are expectations and values are dictionaries
        containing arguments for those expectations. The expected arguments
        are "columns" and "kwargs".

    Raises
    ------
    ValueError
        If any of the following conditions are met:
        - An expectation is not of type `gxe.expectation.MetaExpectation`.
        - The dictionary for an expectation is missing required keys.
        - The "columns" list is empty.

    """
    args_expected = ["columns", "kwargs"]
    for index, (expectation, dict_args) in enumerate(dict_suite.items()):
        if type(expectation) is not gxe.expectation.MetaExpectation:
            msg = (
                f"Expectation at index {index} is not a valid expectation type."
                f" Passed expectation: {expectation}"
            )
            raise ValueError(msg)
        if not check_dict_for_keys(dict_args, args_expected):
            missing_args = set(args_expected) - set(dict_args.keys())
            msg = (
                "Passed suite dictionary is missing the following arguments: "
                f"{missing_args} at index {index}."
            )
            raise ValueError(msg)


def populate_expectation_suite(
    expectation_suite: ExpectationSuite,
    dict_suite: dict[MetaExpectation, dict],
) -> ExpectationSuite:
    """Populate the provided expectation suite with expectations from the suite dictionary.

    Parameters
    ----------
    expectation_suite : ExpectationSuite
        The expectation suite to be populated.
    dict_suite : dict of MetaExpectation to dict
        A dictionary where keys are expectations and values are dictionaries
        containing arguments for those expectations. The expected arguments
        are "columns" and "kwargs".

    Returns
    -------
    ExpectationSuite
        The populated expectation suite.

    """
    for expectation, dict_args in dict_suite.items():
        if "columns" in dict_args:
            if dict_args["columns"]:
                for column in dict_args["columns"]:
                    expectation_suite.add_expectation(
                        expectation(column=column, **dict_args["kwargs"])
                    )
        else:
            expectation_suite.add_expectation(expectation(**dict_args["kwargs"]))

    return expectation_suite
