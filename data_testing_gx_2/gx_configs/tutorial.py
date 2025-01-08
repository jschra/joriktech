from great_expectations import expectations as gxe

from gx_configs.gx_config import GXValidationConfig

LIST_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]

gx_tutorial_config = GXValidationConfig(
    data_source="pandas",
    data_asset="tutorial_data",
    batch_definition="pandas_tutorial",
    example_data="data/yellow_tripdata_2021-11.csv",
    expectation_suite="tutorial",
    validation_definition="pandas-validation",
    checkpoint="pandas",
    expectations={
        gxe.ExpectTableColumnsToMatchSet: {
            "columns": None,
            "kwargs": {"column_set": LIST_COLUMNS},
        },
        gxe.ExpectColumnValuesToBeInSet: {
            "columns": ["VendorID"],
            "kwargs": {"value_set": [1, 2]},
        },
        gxe.ExpectColumnValuesToNotBeNull: {
            "columns": LIST_COLUMNS,
            "kwargs": {},
        },
        gxe.ExpectColumnValuesToMatchRegex: {
            "columns": ["tpep_pickup_datetime", "tpep_dropoff_datetime"],
            "kwargs": {
                "regex": (
                    r"^(?:19|20)\d{2}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]) "
                    r"(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d$"
                ),
            },
        },
        gxe.ExpectColumnValuesToBeBetween: {
            "columns": ["passenger_count", "trip_distance", "tip_amount"],
            "kwargs": {"min_value": 0},
        },
    },
)
