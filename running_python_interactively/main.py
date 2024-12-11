# -- Imports
import random

import boto3

from aws_helpers import (extract_prefixes_from_pages,
                         get_object_pages_at_prefix, load_s3_file_to_dataframe,
                         retrieve_aws_env_vars)

# -- Constants
S3_BUCKET = "ds-tutorial-data"
PREFIX_DATA = "taxi-data/"

# -- Initialize objects
aws_vars = retrieve_aws_env_vars()
s3 = boto3.client("s3", **aws_vars)


def main():
    # -- 1. Scan S3 bucket for data to be loaded
    pages = get_object_pages_at_prefix(s3, S3_BUCKET, PREFIX_DATA)
    prefixes = extract_prefixes_from_pages(pages)
    csv_objects = [pref for pref in prefixes if ".csv" in pref]

    # -- 2. Load the latest file and call commands to show interactive terminal
    file_to_load = csv_objects[-1]
    df_test = load_s3_file_to_dataframe(s3, S3_BUCKET, file_to_load)
    df_test.head()
    df_test.describe()
    df_test["fare_amount"].hist()

    # -- 3. Break a loop due to an error and show debugging steps
    prefixes_shuffled = prefixes.copy()
    random.shuffle(prefixes_shuffled)
    for index, pref in enumerate(prefixes_shuffled):
        _ = load_s3_file_to_dataframe(s3, S3_BUCKET, pref)


if __name__ == "__main__":
    main()
