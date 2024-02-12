from pathlib import Path
from geochemxl.converter import convert

# from pyshacl import validate
# conforms, results_graph, results_text = validate("../basic.ttl", shacl_graph="/Users/nick/Work/gsq/gsq-geochem/profiles/core/validator.ttl")
# print(results_graph.serialize())
# print()
# print(results_text)


def test_conversion_error_01():
    wb = Path(__file__).parent / "data" / "3.0" / "GeochemXL-3.0-integration.xlsx"
    converts, results_graph, results_text = convert(wb)

    assert converts


def test_conversion_error_02():
    wb = Path(__file__).parent / "data" / "3.0" / "GeochemXL-3.0-integration-errors.xlsx"
    converts, results_graph, results_text = convert(wb)

    assert results_text == """Conversion Report
Converts: False
Results (1):
Conversion Violation: 
    The value XXX for DRILLHOLE_ID in row 11 of sheet DRILLHOLE_LITHOLOGY is not present on sheet DRILLHOLE_LOCATION in the DRILLHOLE_ID column, as required. Should be one of DEF123"""
