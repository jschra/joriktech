# -- Imports
from __future__ import annotations

import os
from io import StringIO
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import boto3

import pandas as pd


def retrieve_aws_env_vars() -> dict:
    """Retrieve AWS environment variables from the environment."""
    return {
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
        "aws_session_token": os.environ.get("AWS_SESSION_TOKEN"),
    }


def get_object_pages_at_prefix(
    s3_client: boto3.client("s3"),
    bucket_name: str,
    prefix: str,
    delimiter: str = "/",
) -> list:
    """Get a list of objects within an S3 bucket at a specific prefix.

    Returns a list of results from running the paginator, as the API can only return
    1,000 keys per iteration. Hence if more than 1,000 objects exist under a specific
    prefix, multiple pages of results are returned by this function.

    Parameters
    ----------
    s3_client : boto3.client
        Instantiated s3 client using boto3
    bucket_name : str
        Name of bucket to query
    prefix : str
        Prefix to query
    delimiter : str, optional
        Delimiter used in prefixes, by default "/"

    Returns
    -------
    list
        List of page results from running the paginator

    """
    # Create a paginator to handle multiple pages from list_objects_v2
    paginator = s3_client.get_paginator("list_objects_v2")

    return list(
        paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter=delimiter)
    )


def flatten_list(nested_list: list) -> list:
    """Flattens a nested list.

    Parameters
    ----------
    nested_list : list
        A list which may contain nested lists.

    Returns
    -------
    list
        A flattened version of the input list.

    """
    flattened = []

    for element in nested_list:
        if isinstance(element, list):
            flattened.extend(flatten_list(element))
        else:
            flattened.append(element)

    return flattened


def extract_prefixes_from_pages(
    list_pages: list,
) -> list:
    """Extract list of objects from the results of get_object_pages_at_prefix."""
    nested_list = [[page["Key"] for page in pages["Contents"]] for pages in list_pages]
    return flatten_list(nested_list)


def load_s3_file_to_dataframe(
    s3_client: boto3.client("s3"),
    bucket_name: str,
    file_key: str,
    file_encoding: str = "utf-8",
) -> pd.DataFrame:
    """Load a specific file from an S3 bucket and return it as a pandas DataFrame.

    Parameters
    ----------
    s3_client : boto3.client
        The boto3 S3 client instance to use for accessing the S3 bucket.
    bucket_name : str
        The name of the S3 bucket.
    file_key : str
        The key (path) to the file within the S3 bucket.
    file_encoding : str, optional
        The encoding of the file, by default "utf-8".

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the data from the specified S3 file.

    """
    # Fetch the file from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    file_content = response["Body"].read().decode(file_encoding)

    return pd.read_csv(StringIO(file_content))
