import csv
import io
from typing import Optional
from pathlib import Path
import os.path
from frictionless import Package, Pipeline, Resource, Schema, Step, FrictionlessException, Report, transform, steps, \
    validate
from tabulate import tabulate

DEFAULT_SCHEMA_FILE_NAME = "isamples_simple_schema.json"
ISAMPLES_SIMPLE_SCHEMA = None
DEFAULT_TEMPLATE_FILE_NAME = "isamples_simple_template.csv"
ISAMPLES_SIMPLE_TEMPLATE = None


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


def isamples_simple_schema() -> Schema:
    """
    Constructs the iSamples built-in schema using the bundled JSON
    Returns: The frictionless iSamples simple Schema
    """
    global ISAMPLES_SIMPLE_SCHEMA
    if ISAMPLES_SIMPLE_SCHEMA is None:
        p = Path(__file__)
        schema_json_path = os.path.join(p.parent, DEFAULT_SCHEMA_FILE_NAME)
        ISAMPLES_SIMPLE_SCHEMA = check_valid_schema_json(schema_json_path)
    return ISAMPLES_SIMPLE_SCHEMA


def isamples_simple_template() -> Package:
    global ISAMPLES_SIMPLE_TEMPLATE
    if ISAMPLES_SIMPLE_TEMPLATE is None:
        p = Path(__file__)
        template_file_path = os.path.join(p.parent, DEFAULT_TEMPLATE_FILE_NAME)
        ISAMPLES_SIMPLE_TEMPLATE = create_isamples_package(isamples_simple_schema(), template_file_path)
    return ISAMPLES_SIMPLE_TEMPLATE


def report_errors_as_str(report: Report) -> str:
    errors = report.flatten(['code', 'message'])
    return tabulate(errors, headers=['code', 'message'])


class _PopulateIdentifiersStep(Step):
    def __init__(self, identifiers: list[str]):
        super().__init__()
        self._identifiers = identifiers

    def transform_resource(self, resource):
        current = resource.to_copy()

        # This function is called to replace the data contained in the resource.
        def data():
            with current:
                column_length = 0
                index = 0
                template_row = None
                # The first two rows in the template are the column titles and a placeholder row.
                for list in current.list_stream:
                    if index == 0:
                        # Column titles, return as-is.
                        column_length = len(list)
                        yield list
                    elif index == 1:
                        # Placeholder row, save it for later to use when we populate the latter rows
                        template_row = list
                    index += 1
                # Now add a row for each identifier
                for identifier in self._identifiers:
                    empty_row_with_identifier = template_row.copy()
                    # Replace the identifier column (always going to be the first column) with the current id
                    empty_row_with_identifier[0] = identifier
                    # Then yield the modified list
                    yield empty_row_with_identifier

        # Replace the data by setting a custom function that generates the data.
        resource.data = data

def insert_identifiers_into_template(identifiers: list[str]) -> str:
    source = isamples_simple_template()
    target = transform(source.resources[0], steps=[_PopulateIdentifiersStep(identifiers)])
    stream = io.StringIO()
    writer = csv.writer(stream)
    for number, row in enumerate(target.read_rows()):
        if number == 0:
            writer.writerow(row.field_names)
        writer.writerow(row.to_list())
    result = stream.getvalue().rstrip("\r\n")
    return result