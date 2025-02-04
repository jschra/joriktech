import great_expectations as gx
import pandas as pd
from gx_configs import gx_tutorial_config
from helpers_gx import (
    add_or_update_gx_artifact,
    populate_expectation_suite,
    validate_suite_dictionary,
)

# -- Load data for configuration
df_configure = pd.read_csv(gx_tutorial_config.example_data)

# -- 1. Initialize GX for configuration
context = gx.get_context(mode="file")

data_source = context.data_sources.add_or_update_pandas(
    name=gx_tutorial_config.data_source,
)
data_asset = data_source.add_dataframe_asset(name=gx_tutorial_config.data_asset)
batch_definition = data_asset.add_batch_definition_whole_dataframe(
    gx_tutorial_config.batch_definition,
)

# -- 2. Configure expectation suite to be called over runtime data later
expectation_suite = gx.ExpectationSuite(name=gx_tutorial_config.expectation_suite)
expectation_suite = add_or_update_gx_artifact(
    artifact_factory=context.suites,
    artifact=expectation_suite,
)
_ = validate_suite_dictionary(gx_tutorial_config.expectations)
expectation_suite = populate_expectation_suite(
    expectation_suite,
    gx_tutorial_config.expectations,
)

# -- 2.3. Evaluate results on test dataset
batch_parameters = {"dataframe": df_configure}
batch = batch_definition.get_batch(batch_parameters=batch_parameters)
validation_results = batch.validate(expectation_suite)

# -- 3. Bundle suite and batch into validation definition and checkpoint w/ bundled
# --    actions for easy execution later
validation_definition = gx.ValidationDefinition(
    data=batch_definition,
    suite=expectation_suite,
    name=gx_tutorial_config.validation_definition,
)
validation_definition = add_or_update_gx_artifact(
    artifact_factory=context.validation_definitions,
    artifact=validation_definition,
)

action_list = [
    gx.checkpoint.UpdateDataDocsAction(
        name="update_all_data_docs",
    ),
]
checkpoint = gx.Checkpoint(
    name=gx_tutorial_config.checkpoint,
    validation_definitions=[validation_definition],
    actions=action_list,
    result_format={"result_format": "SUMMARY"},
)
checkpoint = add_or_update_gx_artifact(
    artifact_factory=context.checkpoints,
    artifact=checkpoint,
)

# -- 4. Run checkpoint to validate if everything works properly
file_identifier = gx_tutorial_config.example_data.split("/")[-1]
runid = gx.RunIdentifier(run_name=f"Configuration run - {file_identifier}")
results = checkpoint.run(batch_parameters=batch_parameters, run_id=runid)
