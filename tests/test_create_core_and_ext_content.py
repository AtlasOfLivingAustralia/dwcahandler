import glob
import os
from operator import attrgetter
import pytest
import pandas as pd
from pandas import testing as pdtest
from dwcahandler.dwca import CSVEncoding, CsvFileType, CoreOrExtType, MetaElementTypes
from dwcahandler.dwca.core_dwca import Dwca


single_csv_occ_test = {"file_paths": ['./input_files/occurrence/occ_file1.csv'],
                       "delimiter": ","}
multiple_csv_occ_test = {"file_paths": glob.glob(os.path.join("./input_files/occurrence", "*.csv")),
                         "delimiter": ","}
multiple_tsv_occ_test = {"file_paths": glob.glob(os.path.join("./input_files/occurrence", "*.tsv")),
                         "delimiter": "\t"}
duplicates_csv_occ_test = {"file_paths": single_csv_occ_test["file_paths"] + multiple_csv_occ_test["file_paths"],
                           "delimiter": ","}
csv_occ_with_space = {"file_paths": ['./input_files/occurrence/occ_file1.csv', './input_files/sample/occ_header_with_space.csv'],
                       "delimiter": ","}
multimedia_with_space = {"file_paths": ['./input_files/multimedia/multimedia_file.csv', './input_files/sample/multimedia_header_with_space.csv'],
                      "delimiter": ","}


def get_expected_combined_occ_df(file_paths: list, keys: list, delimiter: str = ","):
    all_records_df = pd.DataFrame()
    dfs = [pd.read_csv(f, dtype='str', delimiter=delimiter) for f in file_paths]
    for df in dfs:
        all_records_df = pd.concat([all_records_df, df], ignore_index=True)
    all_records_df.drop_duplicates(inplace=True)
    all_records_df.set_index(keys=keys, drop=False, inplace=True)
    return all_records_df


@pytest.fixture
def test_case(request):
    yield {"file_type": CsvFileType(files=request.param["file_paths"],
                                    type='occurrence',
                                    keys=['catalogNumber'],
                                    csv_encoding=CSVEncoding(csv_delimiter=request.param["delimiter"])),
           "expected_result": get_expected_combined_occ_df(file_paths=request.param["file_paths"],
                                                           keys=['catalogNumber'],
                                                           delimiter=request.param["delimiter"])}


class TestExtractData:
    """
    Test for extract records from csv or tsv files in Dwca class
    single_csv_files, csv_occ_files, tsv_occ_files, duplicates_csv_occ_test
    """
    @pytest.mark.parametrize("test_case",
                             [single_csv_occ_test, multiple_csv_occ_test,
                              multiple_tsv_occ_test, duplicates_csv_occ_test],
                             indirect=True)
    def test_extract_csv_core_content(self, test_case: dict):
        """
        Parameterized test for csv or tsv input files and check the meta content
        which is used to build the meta.xml
        Scenario 1: Single occurrence csv file
        Scenario 2: Multiple occurrence csv file (with added column and rows)
        Scenario 3: Single occurrence tsv file
        Scenario 4: Multiple occurrence tsv file (with added column and rows)
        """

        dwca_creator = Dwca()

        dwca_creator.extract_csv_content(csv_info=test_case['file_type'],
                                         core_ext_type=CoreOrExtType.CORE)

        # Drop id field from testing
        pd.testing.assert_frame_equal(dwca_creator.core_content.df_content.drop(
                                        columns=['id']), test_case['expected_result'])

        meta_columns = list(map(attrgetter('field_name'),
                                dwca_creator.meta_content.meta_elements[0].fields))

        assert dwca_creator.core_content.df_content.columns.to_list() == meta_columns

        assert (dwca_creator.meta_content.meta_elements[0].meta_element_type.type ==
                MetaElementTypes.get_element('occurrence'))

    def test_extract_csv_ext_content(self):
        """
        Test extract records from csv for extension content
        """

        dwca_creator = Dwca()

        dwca_creator.extract_csv_content(csv_info=CsvFileType(files=multiple_csv_occ_test['file_paths'],
                                                              type='occurrence',
                                                              keys=['catalogNumber'],
                                                              csv_encoding=CSVEncoding(
                                                                  csv_delimiter=multiple_csv_occ_test["delimiter"])),
                                         core_ext_type=CoreOrExtType.CORE)

        multimedia_file_path = './input_files/multimedia/multimedia_file.csv'
        dwca_creator.extract_csv_content(csv_info=CsvFileType(files=[multimedia_file_path],
                                                              type='multimedia',
                                                              keys=['catalogNumber'],
                                                              csv_encoding=CSVEncoding(csv_delimiter=',')),
                                         core_ext_type=CoreOrExtType.EXTENSION)

        # Drop coreid field from testing as this is generated
        pd.testing.assert_frame_equal(dwca_creator.ext_content[0].df_content.drop(
            columns=['coreid']), pd.read_csv(multimedia_file_path))

        meta_columns = list(map(attrgetter('field_name'),
                                dwca_creator.meta_content.meta_elements[1].fields))

        assert sorted(list(map(attrgetter('field_name'), dwca_creator.meta_content.meta_elements[1].fields))) == \
               sorted(['coreid', 'catalogNumber', 'identifier', 'format', 'type'])

        # Test both the meta content extension and extension dataframe is consistent
        assert dwca_creator.ext_content[0].df_content.columns.to_list() == meta_columns

        # Test that the meta content extension if of multimedia type
        assert (dwca_creator.meta_content.meta_elements[1].meta_element_type.type ==
                MetaElementTypes.get_element('multimedia'))

    def test_extract_tsv_ext_content(self):
        """
        Test extract records from csv for extension content
        """

        dwca_creator = Dwca()

        dwca_creator.extract_csv_content(csv_info=CsvFileType(files=multiple_tsv_occ_test['file_paths'],
                                                              type='occurrence',
                                                              keys=['catalogNumber'],
                                                              csv_encoding=CSVEncoding(
                                                                  csv_delimiter=multiple_tsv_occ_test["delimiter"])),
                                         core_ext_type=CoreOrExtType.CORE)

        multimedia_file_path = './input_files/multimedia/multimedia_file.tsv'
        dwca_creator.extract_csv_content(csv_info=CsvFileType(files=[multimedia_file_path],
                                                              type='multimedia',
                                                              keys=['catalogNumber'],
                                                              csv_encoding=CSVEncoding(csv_delimiter='\t')),
                                         core_ext_type=CoreOrExtType.EXTENSION)

        # Drop coreid field from testing as this is generated
        pd.testing.assert_frame_equal(dwca_creator.ext_content[0].df_content.drop(
            columns=['coreid']), pd.read_csv(multimedia_file_path, delimiter='\t'))

        meta_columns = list(map(attrgetter('field_name'),
                                dwca_creator.meta_content.meta_elements[1].fields))

        assert sorted(list(map(attrgetter('field_name'), dwca_creator.meta_content.meta_elements[1].fields))) == \
               sorted(['coreid', 'catalogNumber', 'identifier', 'format', 'type'])

        # Test both the meta content extension and extension dataframe is consistent
        assert dwca_creator.ext_content[0].df_content.columns.to_list() == meta_columns

        # Test that the meta content extension if of multimedia type
        assert (dwca_creator.meta_content.meta_elements[1].meta_element_type.type ==
                MetaElementTypes.get_element('multimedia'))

    def test_extract_csv_with_header_space(self):
        """
        Test extract records from csv with header space
        """

        dwca_creator = Dwca()

        dwca_creator.extract_csv_content(csv_info=CsvFileType(files=csv_occ_with_space['file_paths'],
                                                              type='occurrence',
                                                              keys=['catalogNumber'],
                                                              csv_encoding=CSVEncoding(
                                                                  csv_delimiter=csv_occ_with_space["delimiter"])),
                                         core_ext_type=CoreOrExtType.CORE)

        expected_column_list = ["id", "catalogNumber", "basisOfRecord", "scientificName",
                                "license","decimalLatitude","decimalLongitude"]
        assert set(dwca_creator.core_content.df_content.columns) == set(expected_column_list)
        assert len(dwca_creator.core_content.df_content) == 5
        pdtest.assert_series_equal(dwca_creator.core_content.df_content["catalogNumber"],
                               pd.Series(["C1", "C2", "C3", "C4", "C5"], dtype=str, name='catalogNumber'),
                               check_index_type=False, check_index=False)

    def test_extract_csv_ext_with_header_space(self):
        """
        Test extract records from multimedia csv with header space
        """

        dwca_creator = Dwca()

        dwca_creator.extract_csv_content(csv_info=CsvFileType(files=csv_occ_with_space['file_paths'],
                                                              type='occurrence',
                                                              keys=['catalogNumber'],
                                                              csv_encoding=CSVEncoding(
                                                                  csv_delimiter=csv_occ_with_space["delimiter"])),
                                         core_ext_type=CoreOrExtType.CORE)

        dwca_creator.extract_csv_content(csv_info=CsvFileType(files=multimedia_with_space['file_paths'],
                                                              type='multimedia',
                                                              keys=['catalogNumber'],
                                                              csv_encoding=CSVEncoding(csv_delimiter=',')),
                                         core_ext_type=CoreOrExtType.EXTENSION)

        expected_column_list = ["id", "catalogNumber", "basisOfRecord", "scientificName",
                                "license","decimalLatitude","decimalLongitude"]
        assert set(dwca_creator.core_content.df_content.columns) == set(expected_column_list)
        assert len(dwca_creator.core_content.df_content) == 5
        pdtest.assert_series_equal(dwca_creator.core_content.df_content["catalogNumber"],
                               pd.Series(["C1", "C2", "C3", "C4", "C5"], dtype=str, name="catalogNumber"),
                               check_index_type=False, check_index=False)

        expected_column_list = ["coreid", "catalogNumber", "identifier", "format", "type"]
        assert set(dwca_creator.ext_content[0].df_content.columns) == set(expected_column_list)
        assert len(dwca_creator.ext_content[0].df_content) == 5
        pdtest.assert_series_equal(dwca_creator.ext_content[0].df_content["catalogNumber"],
                               pd.Series(["C1", "C2", "C3", "C4", "C5"], dtype=str, name="catalogNumber"),
                               check_index_type=False, check_index=False)
