import great_expectations as gx
import pandas as pd

# -- Set GX constants for artifact creation
NAME_DATA_SOURCE = "pandas"
NAME_DATA_ASSET = "tutorial_data"
NAME_BATCH_DEF = "pandas_tutorial"
NAME_EXPECTATION_SUITE = "pandas_tutorial"
NAME_VALIDATION_DEF = "pandas_validation"
NAME_CHECKPOINT = "pandas"

FILE_CONFIGURE = "data/yellow_tripdata_2021-11.csv"

# -- Load data for configuration
df_configure = pd.read_csv(FILE_CONFIGURE)

# -- 1. Initialize GX for configuration & set up in-memory source
context = gx.get_context(mode="file")

data_source = context.data_sources.add_pandas(name=NAME_DATA_SOURCE)
data_asset = data_source.add_dataframe_asset(name=NAME_DATA_ASSET)
batch_definition = data_asset.add_batch_definition_whole_dataframe(NAME_BATCH_DEF)

# -- 2. Configure expectation suite to be called over runtime data later
expectation_suite = gx.ExpectationSuite(name=NAME_EXPECTATION_SUITE)
expectation_suite = context.suites.add(expectation_suite)

# -- 2.1. Define table level expectations
columns = list(df_configure.columns)
expectation = gx.expectations.ExpectTableColumnsToMatchSet(column_set=columns)
expectation_suite.add_expectation(expectation)

# -- 2.2. Define column level expectations
# -- 2.2.1. Ensure vendor ID is either 1 or 2
expected_values = [1, 2]
expectation = gx.expectations.ExpectColumnValuesToBeInSet(
    column="VendorID",
    value_set=expected_values,
)
expectation_suite.add_expectation(expectation)

# -- 2.2.2. Validate that all columns have non-null values
for column in columns:
    expectation = gx.expectations.ExpectColumnValuesToNotBeNull(column=column)
    expectation_suite.add_expectation(expectation)

# -- 2.2.3. Validate that pickup and dropoff datetimes are in the correct format
date_columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
DATE_PATTERN = (
    r"^(?:19|20)\d{2}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]) "
    r"(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d$"
)
for date_column in date_columns:
    expectation = gx.expectations.ExpectColumnValuesToMatchRegex(
        column=date_column,
        regex=DATE_PATTERN,
    )
    expectation_suite.add_expectation(expectation)

# -- 2.2.4. Validate non-zero columns
numeric_columns = [
    "passenger_count",
    "trip_distance",
    "tip_amount",
]
for numeric_column in numeric_columns:
    expectation = gx.expectations.ExpectColumnValuesToBeBetween(
        column=numeric_column,
        min_value=0,
    )
    expectation_suite.add_expectation(expectation)

# -- 2.3. Evaluate results on test dataset
batch_parameters = {"dataframe": df_configure}
batch = batch_definition.get_batch(batch_parameters=batch_parameters)
validation_results = batch.validate(expectation_suite)

# -- 3. Bundle suite and batch into validation definition and checkpoint w/ bundled
# --    actions for easy execution later
validation_definition = gx.ValidationDefinition(
    data=batch_definition,
    suite=expectation_suite,
    name=NAME_VALIDATION_DEF,
)
_ = context.validation_definitions.add(validation_definition)

action_list = [
    gx.checkpoint.UpdateDataDocsAction(
        name="update_all_data_docs",
    ),
]
checkpoint = gx.Checkpoint(
    name=NAME_CHECKPOINT,
    validation_definitions=[validation_definition],
    actions=action_list,
    result_format={
        "result_format": "COMPLETE",
        "unexpected_index_column_names": ["event_id"],
    },
)
_ = context.checkpoints.add(checkpoint)

# -- 4. Run checkpoint to validate if everything works properly
file_identifier = FILE_CONFIGURE.split("/")[-1]
runid = gx.RunIdentifier(run_name=f"Configuration run - {file_identifier}")
results = checkpoint.run(batch_parameters=batch_parameters, run_id=runid)
