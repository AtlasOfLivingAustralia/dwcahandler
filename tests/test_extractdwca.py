import glob
import os
from operator import attrgetter
import pytest
import pandas as pd
from dwcahandler.dwca import CSVEncoding, CsvFileType, CoreOrExtType, MetaElementTypes
from dwcahandler.dwca.core_dwca import Dwca


single_csv_occ_test = {"file_paths":['./input_files/occurrence/occ_file1.csv'],
                       "delimiter":","}
multiple_csv_occ_test = {"file_paths":glob.glob(os.path.join("./input_files/occurrence", "*.csv")),
                         "delimiter":","}
multiple_tsv_occ_test = {"file_paths":glob.glob(os.path.join("./input_files/occurrence", "*.tsv")),
                         "delimiter":"\t"}
duplicates_csv_occ_test = {"file_paths":single_csv_occ_test["file_paths"] +
                                        multiple_csv_occ_test["file_paths"],
                           "delimiter":","}


def get_expected_combined_occ_df(file_paths: list, keys: list, delimiter: str = ","):
    all_records_df = pd.DataFrame()
    dfs = [pd.read_csv(f, dtype='str', delimiter=delimiter) for f in file_paths]
    for df in dfs:
        all_records_df = pd.concat([all_records_df, df], ignore_index=True)
    all_records_df.drop_duplicates(inplace=True)
    all_records_df.set_index(keys=keys, drop=False, inplace=True)
    return all_records_df


@pytest.fixture
def test_case (request):
    yield {"file_type": CsvFileType(files=request.param["file_paths"],
                           type='occurrence',
                           keys=['catalogNumber'],
                           csv_encoding=CSVEncoding(csv_delimiter=request.param["delimiter"])),
          "expected_result": get_expected_combined_occ_df(file_paths=request.param["file_paths"],
                                                          keys=['catalogNumber'],
                                                          delimiter=request.param["delimiter"])}

class TestExtractData():
    """
    Test for extract records from csv or tsv files in Dwca class
    single_csv_files, csv_occ_files, tsv_occ_files, duplicates_csv_occ_test
    """
    @pytest.mark.parametrize("test_case",
                             [single_csv_occ_test, multiple_csv_occ_test,
                              multiple_tsv_occ_test, duplicates_csv_occ_test],
                             indirect=True)
    def test_extract_csv_content(self, test_case):
        """
        Test the content of csv or text is extracted and populated the meta content
        :return:

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
