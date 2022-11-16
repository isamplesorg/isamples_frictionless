import isamples_frictionless
import csv


def test_insert_identifiers_into_template():
    formatted_csv = isamples_frictionless.insert_identifiers_into_template(["123", "456"])
    assert formatted_csv is not None
    print("formatted csv is")
    print(formatted_csv)
    for row in csv.reader(formatted_csv.splitlines()):
        assert row is not None
        print(f"row is {row}")