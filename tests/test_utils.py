from geochemxl.utils import *

CONCEPTS_COMBINED_GRAPH = Graph().parse(Path(__file__).parent / "data" / "3.0" / "concepts-combined-3.0.ttl")


def test__get_codelist_id_for_code():
    assert "LENGTH" in get_codelist_ids_for_code("m", CONCEPTS_COMBINED_GRAPH)

    assert "LOC_SURVEY_TYPE" in get_codelist_ids_for_code("DGPS", CONCEPTS_COMBINED_GRAPH)
