import json
from typing import Optional

from frictionless import Package, Resource, Schema, FrictionlessException, Report
from tabulate import tabulate

DEFAULT_SCHEMA_FILE_NAME = "isamples_simple_schema.json"


def check_valid_schema_json(schema_file_path: str) -> Optional[Schema]:
    try:
        schema = Schema(schema_file_path)
        return schema
    except FrictionlessException as e:
        print(f"Error constructing frictionless schema: {e}")
        return None


def create_isamples_package(schema: Schema, file_path: str) -> Package:
    """
    Opens the specified file and return it as a Frictionless Package
    Args:
        schema: The frictionless Schema to use to open the file
        file_path: The path to the data file to open

    Returns: A frictionless package representing the file at the specified file path
    """
    data_resource = Resource(source=file_path, schema=schema, trusted=True)
    package = Package(resources=[data_resource], name="isamples", title="isamples", id="isamples", trusted=True)
    return package


def report_errors_as_str(report: Report) -> str:
    errors = report.flatten(['code', 'message'])
    return tabulate(errors, headers=['code', 'message'])
