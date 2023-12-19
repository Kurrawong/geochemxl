import pytest

from geochemxl.convert import *

CONCEPTS_COMBINED_GRAPH = Graph().parse(Path(__file__).parent / "data" / "concepts-combined-3.0.ttl")
TESTS_DIR = Path(__file__).parent


@pytest.fixture()
def make_cc() -> Graph:
    return Graph().parse(TESTS_DIR / "data" / "concepts-combined-3.0.ttl")


class TestExtractSheetDatasetMetadata30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_extract_sheet_dataset_metadata(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-DATASET_METADATA_01_valid.xlsx")
        g, iri = extract_sheet_dataset_metadata(wb, self._make_cc, "3.0")

        assert type(g) == Graph
        assert type(iri) == URIRef

        for o in g.objects(None, SDO.dateModified):
            assert type(o) == Literal
            assert o.value == datetime.datetime(2023, 11, 15)


class TestValidateSheetValidationDictionary30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-VALIDATION_DICTIONARY_01_invalid.xlsx")
        try:
            validate_sheet_validation_dictionary(wb, self._make_cc, "3.0")
        except ConversionError as e:
            assert str(e) == "Code xx in codelist LOC_SURVEY_TYPE on worksheet VALIDATION_DICTIONARY is not known"

    def test_02_valid(self):
        # code xx is defined by user
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-VALIDATION_DICTIONARY_02_valid.xlsx")
        validate_sheet_validation_dictionary(wb, self._make_cc, "3.0")

    def test_03_invalid(self):
        # code xx is incorrectly defined as xy by user
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-VALIDATION_DICTIONARY_03_invalid.xlsx")
        try:
            validate_sheet_validation_dictionary(
                wb, Graph().parse(TESTS_DIR / "data" / "concepts-combined-3.0.ttl"), "3.0")
        except ConversionError as e:
            assert str(e) == "Code xx in codelist LOC_SURVEY_TYPE on worksheet VALIDATION_DICTIONARY is not known"


class TestExtractSheetUserDictionary30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-USER_DICTIONARY_01_valid.xlsx")
        g = extract_sheet_user_dictionary(wb, "3.0")

        concepts_count = 0
        notations = []
        for c in g.subjects(RDF.type, SKOS.Concept):
            concepts_count += 1
            notations.append(str(g.value(subject=c, predicate=SKOS.notation)))
        assert concepts_count == 2

        assert "TESTER" in notations

    def test_02_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-USER_DICTIONARY_02_invalid.xlsx")
        try:
            extract_sheet_user_dictionary(wb, "3.0")
        except ConversionError as e:
            assert str(e) == "You must supply a DESCRIPTION value for each code you define in the USER_DICTIONARY sheet"


class TestExtractSheetUom30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-UNITS_OF_MEASURE_01_valid.xlsx")
        validate_sheet_uom(wb, self._make_cc, "3.0")

    def test_02_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-UNITS_OF_MEASURE_02_invalid.xlsx")
        try:
            validate_sheet_uom(wb, self._make_cc, "3.0")
        except ConversionError as e:
            assert str(e) == "Code rounds in codelist ROTATION on worksheet UNITS_OF_MEASURE is not known"


class TestExtractSheetUserUom30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-USER_UNITS_OF_MEASURE_01_valid.xlsx")
        g, notations = extract_sheet_user_uom(wb, self._make_cc, "3.0")

        concepts_count = 0
        for c in g.subjects(RDF.type, SKOS.Concept):
            for _ in g.triples((c, SKOS.inScheme, URIRef("http://example.com/user-defined-uom"))):
                concepts_count += 1
                notations.append(str(self._make_cc.value(subject=c, predicate=SKOS.notation)))
        assert concepts_count == 2

        assert "BPC" in notations

    def test_02_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-USER_UNITS_OF_MEASURE_01_valid.xlsx")
        try:
            extract_sheet_user_uom(wb, self._make_cc, "3.0")
        except ConversionError as e:
            assert str(e) == "You must supply a DESCRIPTION value for each code you define in the " \
                             "USER_UNITS_OF_MEASURE sheet"


class TestExtractSheetUserSamplePrepCodes30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-USER_SAMPLE_PREP_CODES_01_valid.xlsx")
        g, prep_code_ids = extract_sheet_user_sample_prep_codes(wb, URIRef("http://test.com"), "3.0")

        assert (
                   URIRef("http://test.com/user-ConceptScheme-sample-preparations"),
                   SKOS.hasTopConcept,
                   URIRef("http://test.com/code/D"),
               ) in g

        assert 'A' in prep_code_ids

    def test_02_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-USER_SAMPLE_PREP_CODES_02_invalid.xlsx")
        try:
            extract_sheet_user_sample_prep_codes(wb, URIRef("http://test.com"), "3.0")
        except ConversionError as e:
            assert str(e) == "For each row in the USER_SAMPLE_PREP_CODES worksheet, you must supply a " \
                             "DESCRIPTION value"


class TestExtractSheetUserAssayCodes30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-USER_ASSAY_CODES_01_valid.xlsx")
        g, assay_code_ids = extract_sheet_user_assay_codes(wb, URIRef("http://test.com"), "3.0")

        assert (
                   URIRef("http://test.com/user-ConceptScheme-assays"),
                   SKOS.hasTopConcept,
                   URIRef("http://test.com/code/TTT"),
               ) in g

        assert 'TTT' in assay_code_ids

    def test_02_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-USER_ASSAY_CODES_02_invalid.xlsx")
        try:
            extract_sheet_user_assay_codes(wb, URIRef("http://test.com"), "3.0")
        except ConversionError as e:
            assert str(e) == "For each row in the USER_ASSAY_CODES worksheet, you must supply a " \
                             "DESCRIPTION value"


class TestExtractSheetUserAnalytes30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-USER_ANALYTES_01_valid.xlsx")
        g, analyte_ids = extract_sheet_user_analytes(wb, URIRef("http://test.com"), "3.0")

        assert (
                   URIRef("http://test.com/user-ConceptScheme-analytes"),
                   SKOS.hasTopConcept,
                   URIRef("http://test.com/code/Ag"),
               ) in g

        assert 'Ag' in analyte_ids

    def test_02_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-USER_ANALYTES_02_invalid.xlsx")
        try:
            extract_sheet_user_analytes(wb, URIRef("http://test.com"), "3.0")
        except ConversionError as e:
            assert str(e) == "For each row in the USER_ANALYTES worksheet, you must supply a " \
                             "DESCRIPTION value"


class TestExtractSheetUserLaboratories30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-USER_LABORATORIES_01_valid.xlsx")
        g, lab_names_and_ids = extract_sheet_user_laboratories(wb, URIRef("http://test.com"), "3.0")

        assert (
                   URIRef("http://test.com/lab/ABC-Corp-GC"),
                   RDF.type,
                   SDO.Organization,
               ) in g

    def test_02_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-USER_LABORATORIES_02_invalid.xlsx")
        try:
            extract_sheet_user_laboratories(wb, URIRef("http://test.com"), "3.0")
        except ConversionError as e:
            assert str(e) == "For each row in the USER_LABORATORIES worksheet, you must supply a " \
                             "LABORATORY_LOCATION value"


class TestExtractSheetTenement30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-TENEMENT_01_valid.xlsx")
        g = extract_sheet_tenement(wb, self._make_cc, URIRef("http://test.com"))

        assert (URIRef("https://linked.data.gov.au/dataset/gsq-tenements/6789"), RDF.type, TENEMENT.Tenement) in g

        assert (
                   URIRef("https://linked.data.gov.au/dataset/gsq-tenements/6789"),
                   TENEMENT.hasProject,
                   Literal("Test Project")
               ) in g

    def test_02_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-TENEMENT_02_invalid.xlsx")
        try:
            extract_sheet_tenement(wb, self._make_cc, URIRef("http://test.com"))
        except ConversionError as e:
            assert str(e) == "For each row in the TENEMENT worksheet, you must supply a TENEMENT_OPERATOR value"

    def test_03_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-TENEMENT_03_invalid.xlsx")
        try:
            extract_sheet_tenement(wb, self._make_cc, URIRef("http://test.com"))
        except ConversionError as e:
            assert str(e) == "The value Hello for GEODETIC_DATUM in row 10 on the TENEMENT worksheet is not within " \
                             "the COORD_SYS_ID lookup list"


class TestExtractSheetDrillholeLocation30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-DRILLHOLE_LOCATION_01_valid.xlsx")
        g, drillhole_ids = extract_sheet_drillhole_location(wb, self._make_cc, URIRef("http://test.com"), "3.0")

        assert (
                   URIRef("https://linked.data.gov.au/dataset/gsq-bores/DEF123"),
                   BORE.hadDrillingMethod,
                   URIRef("https://linked.data.gov.au/def/gsq-geochem/drill-type/box-core")
               ) in g

    def test_02_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-DRILLHOLE_LOCATION_02_invalid.xlsx")
        try:
            extract_sheet_drillhole_location(wb, self._make_cc, URIRef("http://test.com"), "3.0")
        except ConversionError as e:
            assert str(e) == "For each row in the DRILLHOLE_LOCATION worksheet, " \
                             "you must supply a PRE_COLLAR_DEPTH value"


class TestExtractSheetDrillholeSurvey30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-DRILLHOLE_SURVEY_01_valid.xlsx")
        g = extract_sheet_drillhole_survey(wb, self._make_cc, ["DD1234", "DEF123"], URIRef("http://test.com"))

        assert (
                   URIRef("https://linked.data.gov.au/dataset/gsq-bores/DD1234"),
                   BORE.hadSurvey,
                   None
               ) in g

    def test_02_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-DRILLHOLE_SURVEY_02_invalid.xlsx")
        try:
            extract_sheet_drillhole_survey(wb, self._make_cc, ["DD1234", "DEF123"], URIRef("http://test.com"))
        except ConversionError as e:
            assert str(e) == "The value DD1234x for DRILLHOLE_ID in row 10 of sheet DRILLHOLE_SURVEY is not " \
                             "present on sheet DRILLHOLE_LOCATION in the DRILLHOLE_ID column, as required"


class TestExtractSheetDrillholeSample30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-DRILLHOLE_SAMPLE_01_valid.xlsx")
        g, _ = extract_sheet_drillhole_sample(wb, self._make_cc, ["DD1234", "DEF123"], URIRef("http://test.com"))

        assert (
                   URIRef("http://test.com/sample/ABC123"),
                   SOSA.isSampleOf,
                   URIRef("https://linked.data.gov.au/dataset/gsq-bores/DEF123"),
               ) in g

    def test_02_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-DRILLHOLE_SAMPLE_02_invalid.xlsx")
        try:
            extract_sheet_drillhole_sample(wb, self._make_cc, ["DD1234", "DEF123"], URIRef("http://test.com"))
        except ConversionError as e:
            assert str(e) == "For each row in the DRILLHOLE_SAMPLE worksheet, you must supply a TO value"

    def test_03_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-DRILLHOLE_SAMPLE_03_invalid.xlsx")
        try:
            extract_sheet_drillhole_sample(wb, self._make_cc, ["DD1234", "DEF123"], URIRef("http://test.com"))
        except ConversionError as e:
            assert str(e) == "The value 3600 for MAGNETIC_SUSCEPTIBILITY in row 10 of sheet DRILLHOLE_SAMPLE is " \
                             "not negative, as required"


class TestExtractSheetSurfaceSample30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-SURFACE_SAMPLE_01_valid.xlsx")
        g, _ = extract_sheet_surface_sample(wb, self._make_cc, URIRef("http://test.com"))

        assert (
                   URIRef("http://test.com/sample/SSABCD"),
                   SDO.material,
                   URIRef("https://linked.data.gov.au/def/gsq-geochem/sample-material/sediment"),
               ) in g

    def test_02_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-SURFACE_SAMPLE_02_invalid.xlsx")
        try:
            extract_sheet_surface_sample(wb, self._make_cc, URIRef("http://test.com"))
        except ConversionError as e:
            assert str(e) == "The value hello for DISPATCH_DATE in row 12 of sheet SURFACE_SAMPLE " \
                             "is not a date as required"


class TestExtractSheetSamplePreparation30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-SAMPLE_PREPARATION_01_valid.xlsx")
        labs = {
            "ABC Corp (GC)": "abc-corp-gc",
            "ABC Corp": "abc-corp",
            "DEF Corp": "def-corp",
        }
        sample_ids = [
            "SSABCD",
            "SSABCE",
            "SSABCF"
        ]
        g, _ = extract_sheet_sample_preparation(
            wb, labs, ["A", "B", "C", "D"], ["TTTT"], sample_ids, URIRef("http://test.com"), "3.0")

        has_expected_foi = False
        for oc in g.subjects(RDF.type, SOSA.ObservationCollection):
            for o in g.objects(oc, SOSA.hasMember):
                for foi in g.objects(o, SOSA.hasFeatureOfInterest):
                    if foi == URIRef("http://test.com/sample/SSABCE"):
                        has_expected_foi = True

        assert has_expected_foi

    def test_02_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-SAMPLE_PREPARATION_02_invalid.xlsx")
        labs = {
            "ABC Corp (GC)": "abc-corp-gc",
            "ABC Corp": "abc-corp",
            "DEF Corp": "def-corp",
        }
        sample_ids = [
            "SSABCD",
            "SSABCE",
            "SSABCF"
        ]
        try:
            extract_sheet_sample_preparation(
                wb, labs, ["A", "B", "C", "D"], ["TTTT"], sample_ids, URIRef("http://test.com"), "3.0")
        except ConversionError as e:
            assert str(e) == "The value TTTX for ASSAY_CODE in row 15 of sheet SAMPLE_PREPARATION is not " \
                             "defined in the USER_ASSAY_CODES worksheet as required"


class TestExtractSheetGeochemistryMeta30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-GEOCHEMISTRY_META_01_valid.xlsx")
        labs = {
            "ABC Corp (GC)": "abc-corp-gc",
            "GeoChem Labs Pty Ltd": "geochem-labs",
            "DEF Corp": "def-corp",
        }
        job_numbers = [
            "JOB_27"
        ]
        g = extract_sheet_geochemistry_meta(
            wb, job_numbers, labs, ["TTTT"], ["Au"], ["ppm", "ppb"], self._make_cc, URIRef("http://test.com"), "3.0")

        assert (None, SDO.marginOfError, Literal("0.05")) in g

    def test_02_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-GEOCHEMISTRY_META_02_invalid.xlsx")
        labs = {
            "ABC Corp (GC)": "abc-corp-gc",
            "GeoChem Labs Pty Ltd": "geochem-labs",
            "DEF Corp": "def-corp",
        }
        job_numbers = [
            "JOB_27"
        ]
        try:
            extract_sheet_geochemistry_meta(
                wb, job_numbers, labs, ["TTTT"], ["Au"], ["ppm", "ppb"], self._make_cc, URIRef("http://test.com"),
                "3.0")
        except ConversionError as e:
            assert str(e) == "For each row in the GEOCHEMISTRY_META worksheet, you must supply a ACCURACY value"


class TestExtractSheetSampleGeochemistry30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-SAMPLE_GEOCHEMISTRY_01_valid.xlsx")
        g = extract_sheet_sample_geochemistry(
            wb, ["JOB_27"], ["SSABCD"], ["TTTT"], ["Ag", "As", "Au"], URIRef("http://test.com"), "3.0")

        assert (None, SOSA.observedProperty, URIRef("http://test.com/analyteCode/Au")) in g

    def test_02_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-SAMPLE_GEOCHEMISTRY_02_invalid.xlsx")
        try:
            extract_sheet_sample_geochemistry(
                wb, ["JOB_27"], ["SSABCD"], ["TTTT"], ["Ag", "As", "Au"], URIRef("http://test.com"), "3.0")
        except ConversionError as e:
            assert str(e) == "The value JB27 for JOB_NUMBER in row 15 of sheet SAMPLE_GEOCHEMISTRY is not present " \
                             "in the SAMPLE_PREPARATION job numbers worksheet as required"


class TestExtractSheetQaqcMeta30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-QAQC_META_01_valid.xlsx")
        labs = {
            "ABC Corp (GC)": "abc-corp-gc",
            "GeoChem Labs Pty Ltd": "geochem-labs",
            "DEF Corp": "def-corp",
        }
        job_numbers = [
            "JOB_27"
        ]
        g = extract_sheet_qaqc_meta(
            wb, job_numbers, labs, ["TTTT"], ["Au"], ["ppm", "ppb"], self._make_cc, URIRef("http://test.com"), "3.0")

        assert (None, SDO.marginOfError, Literal("0.05")) in g

    def test_02_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-QAQC_META_02_invalid.xlsx")
        labs = {
            "ABC Corp (GC)": "abc-corp-gc",
            "GeoChem Labs Pty Ltd": "geochem-labs",
            "DEF Corp": "def-corp",
        }
        job_numbers = [
            "JOB_27"
        ]
        try:
            extract_sheet_qaqc_meta(
                wb, job_numbers, labs, ["TTTT"], ["Au"], ["ppm", "ppb"], self._make_cc, URIRef("http://test.com"),
                "3.0")
        except ConversionError as e:
            assert str(e) == "For each row in the QAQC_META worksheet, you must supply a ACCURACY value"


class TestExtractSheetQaqcGeochemistry30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-QAQC_GEOCHEMISTRY_01_valid.xlsx")
        g = extract_sheet_qaqc_geochemistry(
            wb, ["JOB_27"], ["DEF123", "SSABCD"], ["TTTT"], ["Ag", "As", "Au"], self._make_cc,
            URIRef("http://test.com"), "3.0")

        assert (None, SOSA.observedProperty, URIRef("http://test.com/analyteCode/Au")) in g
        assert (URIRef("http://test.com/sample/DEF123"), SOSA.isSampleOf, URIRef("http://test.com/sample/S54321")) in g

    def test_02_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-QAQC_GEOCHEMISTRY_02_invalid.xlsx")
        try:
            extract_sheet_qaqc_geochemistry(
                wb, ["JOB_27"], ["DEF123", "SSABCD"], ["TTTT"], ["Ag", "As", "Au"], self._make_cc,
                URIRef("http://test.com"), "3.0")
        except ConversionError as e:
            assert str(e) == "For each row in the QAQC_GEOCHEMISTRY worksheet, you must supply a STANDARD_ID value"


class TestExtractSheetSamplePxrf30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-SAMPLE_PXRF_01_valid.xlsx")
        g = extract_sheet_sample_pxrf(
            wb, ["SS12346", "SS12347"], ["Ag", "As", "Au"], ["ppb", "g/t"],
            self._make_cc, URIRef("http://test.com"), "3.0")

        assert (None, SOSA.observedProperty, URIRef("http://test.com/analyteCode/Au")) in g
        for s in g.subjects(RDF.type, SOSA.Procedure):
            desc = g.value(subject=s, predicate=SDO.description)
            desc: Literal
            assert desc.datatype == RDF.JSON

    def test_02_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-SAMPLE_PXRF_02_invalid.xlsx")
        try:
            extract_sheet_sample_pxrf(
                wb, ["SS12346", "SS12347"], ["Ag", "As", "Au"], ["ppb", "g/t"],
                self._make_cc, URIRef("http://test.com"), "3.0")
        except ConversionError as e:
            assert str(e) == "For each row in the SAMPLE_PXRF worksheet, you must supply a RESULT value"


class TestExtractSheetLithDictionary30:
    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-LITH_DICTIONARY_01_valid.xlsx")
        g, lith_ids = extract_sheet_lith_dictionary(wb, URIRef("http://test.com"), "3.0")

        assert len(lith_ids) == 180

    def test_02_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-LITH_DICTIONARY_02_invalid.xlsx")
        try:
            extract_sheet_lith_dictionary(wb, URIRef("http://test.com"), "3.0")
        except ConversionError as e:
            assert str(e) == "The value for GSQ_CODE_MATCH on row 24 of the worksheet LITH_DICTIONARY must not be null"


class TestExtractSheetMinDictionary30:
    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-MIN_DICTIONARY_01_valid.xlsx")
        g, lith_ids = extract_sheet_min_dictionary(wb, URIRef("http://test.com"), "3.0")

        assert len(lith_ids) == 69

    def test_02_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-MIN_DICTIONARY_02_invalid.xlsx")
        try:
            extract_sheet_min_dictionary(wb, URIRef("http://test.com"), "3.0")
        except ConversionError as e:
            assert str(e) == "The value for GSQ_CODE_MATCH on row 24 of the worksheet MIN_DICTIONARY must not be null"


class TestExtractSheetDrillholeLithology30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        @pytest.fixture(autouse=True)
        def _make_cc(make_cc):
            self._make_cc = make_cc

        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-DRILLHOLE_LITHOLOGY_01_valid.xlsx")
        g = extract_sheet_drillhole_lithology(
            wb, ["DD12346"], ["SL", "OSO"], ["Qz", "Sp", "OL", "Ka", "As"],
            self._make_cc, URIRef("http://test.com"), "3.0")

        no_obs = 0
        for _, _ in g.subject_objects(SOSA.hasMember):
            no_obs += 1

        assert no_obs == 15

    def test_02_invalid(self):
        @pytest.fixture(autouse=True)
        def _make_cc(self, make_cc):
            self._make_cc = make_cc

        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-DRILLHOLE_LITHOLOGY_02_invalid.xlsx")

        try:
            extract_sheet_drillhole_lithology(wb, ["DD12346"], ["SL", "OSO"], ["Qz", "Sp", "OL", "Ka", "As"],
                                              self._make_cc, URIRef("http://test.com"), "3.0")
        except ConversionError as e:
            assert str(e) == "The value 1 for TO in row 10 of sheet DRILLHOLE_LITHOLOGY is not greater or equal " \
                             "to the FROM value as required"

    def test_03_invalid(self):
        @pytest.fixture(autouse=True)
        def _make_cc(self, make_cc):
            self._make_cc = make_cc

        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-DRILLHOLE_LITHOLOGY_03_invalid.xlsx")

        try:
            extract_sheet_drillhole_lithology(wb, ["DD12346"], ["SL", "OSO"], ["Qz", "Sp", "OL", "Ka", "As"],
                                              self._make_cc, URIRef("http://test.com"), "3.0")
        except ConversionError as e:
            assert str(e) == "The value XXX for ALT_TYPE in row 10 on the DRILLHOLE_LITHOLOGY worksheet is not " \
                             "within the ALTERATION lookup list"

    def test_04_invalid(self):
        @pytest.fixture(autouse=True)
        def _make_cc(self, make_cc):
            self._make_cc = make_cc

        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-DRILLHOLE_LITHOLOGY_04_invalid.xlsx")

        try:
            extract_sheet_drillhole_lithology(wb, ["DD12346"], ["SL"], ["Qz", "Sp", "OL", "Ka", "As"], self._make_cc,
                                              URIRef("http://test.com"), "3.0")
        except ConversionError as e:
            assert str(e) == "The value OSO for ROCK_2 in row 10 of sheet DRILLHOLE_LITHOLOGY definedin the " \
                             "worksheet LITH_DICTIONARY in column B"


class TestExtractSheetDrillholeStructure30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-DRILLHOLE_STRUCTURE_01_valid.xlsx")
        g = extract_sheet_drillhole_structure(
            wb, ["DD12346", "DD12347"], self._make_cc, URIRef("http://test.com"), "3.0")

        no_obs = 0
        for _, _ in g.subject_objects(SOSA.hasMember):
            no_obs += 1

        assert no_obs == 14

    def test_02_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-DRILLHOLE_STRUCTURE_02_invalid.xlsx")

        try:
            extract_sheet_drillhole_structure(wb, ["DD12346", "DD12347"], self._make_cc, URIRef("http://test.com"),
                                              "3.0")
        except ConversionError as e:
            assert str(e) == "The value WHAT for STRUCTURE in row 11 on the DRILLHOLE_STRUCTURE worksheet is not " \
                             "within the STRUCTURAL_FEATURE lookup list"

    def test_03_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-DRILLHOLE_STRUCTURE_03_invalid.xlsx")

        try:
            extract_sheet_drillhole_structure(wb, ["DD12346"], self._make_cc, URIRef("http://test.com"), "3.0")
        except ConversionError as e:
            assert str(e) == "The value DD12348 for DRILLHOLE_ID in row 11 of sheet DRILLHOLE_STRUCTURE is not " \
                             "present on sheet DRILLHOLE_LOCATION in the DRILLHOLE_ID column, as required"

    def test_04_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-DRILLHOLE_STRUCTURE_04_invalid.xlsx")

        try:
            extract_sheet_drillhole_structure(wb, ["DD12346", "DD12347", "DD12348"], self._make_cc,
                                              URIRef("http://test.com"), "3.0")
        except ConversionError as e:
            assert str(e) == "The value 500 for ALPHA_ANGLE in row 11 of sheet DRILLHOLE_STRUCTURE is not " \
                      "between 0 and 360 as required"


class TestExtractSheetSurfaceLithology30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        @pytest.fixture(autouse=True)
        def _make_cc(make_cc):
            self._make_cc = make_cc

        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-SURFACE_LITHOLOGY_01_valid.xlsx")
        g = extract_sheet_surface_lithology(
            wb, ["SS12346"], ["TMO"], ["Qz", "Sp", "OL", "Ka", "As"],
            self._make_cc, URIRef("http://test.com"), "3.0")

        no_obs = 0
        for _, _ in g.subject_objects(SOSA.hasMember):
            no_obs += 1

        assert no_obs == 10

    def test_02_invalid(self):
        @pytest.fixture(autouse=True)
        def _make_cc(self, make_cc):
            self._make_cc = make_cc

        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-SURFACE_LITHOLOGY_02_invalid.xlsx")

        try:
            extract_sheet_surface_lithology(wb, ["SS12346"], ["TMO"], ["Qz", "Sp", "OL", "Ka", "As"],
                                              self._make_cc, URIRef("http://test.com"), "3.0")
        except ConversionError as e:
            assert str(e) == "For each row in the SURFACE_LITHOLOGY worksheet, you must supply a COLLECTION_DATE value"


class TestExtractSheetSurfaceStructure30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-SURFACE_STRUCTURE_01_valid.xlsx")
        g = extract_sheet_surface_structure(wb, [], self._make_cc, URIRef("http://test.com"), "3.0")

        no_obs = 0
        for _, _ in g.subject_objects(SOSA.hasMember):
            no_obs += 1

        assert no_obs == 4

    def test_02_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-SURFACE_STRUCTURE_02_invalid.xlsx")

        try:
            extract_sheet_surface_structure(wb, [], self._make_cc, URIRef("http://test.com"),  "3.0")
        except ConversionError as e:
            assert str(e) == "For each row in the SURFACE_STRUCTURE worksheet, you must supply a " \
                             "LOCATION_SURVEY_TYPE value"


class TestExtractSheetReservesResources30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-RESERVES_RESOURCES_01_valid.xlsx")
        g = extract_sheet_reserves_resources(wb, self._make_cc, URIRef("http://test.com"), "3.0")

        assert (
            URIRef("http://test.com/project/ABC-MINE-2"),
            RDF.type,
            SDO.Project
        ) in g

    def test_02_invalid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-RESERVES_RESOURCES_02_invalid.xlsx")

        try:
            extract_sheet_reserves_resources(wb, self._make_cc, URIRef("http://test.com"),  "3.0")
        except ConversionError as e:
            assert str(e) == "For each row in the SURFACE_STRUCTURE worksheet, you must supply a " \
                             "LOCATION_SURVEY_TYPE value"


class TestIntegration30:
    @pytest.fixture(autouse=True)
    def _make_cc(self, make_cc):
        self._make_cc = make_cc

    def test_01_valid(self):
        wb = load_workbook(TESTS_DIR / "data" / "GeochemXL-v3.0-integration_01.xlsx")
        g, dataset_iri = workbook_to_rdf(wb, self._make_cc)

        assert len(g) == 1979

        assert type(dataset_iri) == URIRef
