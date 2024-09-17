
import pandas as pd
from zipfile import ZipFile
from tests import make_meta_xml_str, remove_pretty_print_xml
from tests import make_dwca
from dwcahandler import DwcaHandler, CsvFileType
from io import BytesIO


class TestDeleteContent:

    def test_delete_core_records(self):
        """
        Test for record deletion in occurrence core
        """
        occ_df = pd.DataFrame(data=[["1", "species1", "-30.0000", "144.0000"],
                                    ["2", "species2", "-28.0000", "115.0000"],
                                    ["3", "species3", "-36.0000", "144.308848"]],
                              columns=['occurrenceID', 'scientificName', 'decimalLatitude', 'decimalLongitude'])

        dwca_obj = make_dwca(occ_df)

        delete_df = pd.DataFrame(data=[["2", "species2"],
                                       ["3", "species3"]],
                                 columns=['occurrenceID', 'scientificName'])

        delete_records = CsvFileType(files=delete_df, type='occurrence', keys=['occurrenceID'])

        output_obj = BytesIO()

        DwcaHandler.delete_records(dwca_file=dwca_obj, records_to_delete=delete_records, output_dwca_path=output_obj)

        expected_meta_xml = make_meta_xml_str(occ_df)

        with ZipFile(output_obj, 'r') as zf:
            files = zf.namelist()
            assert 'occurrence.csv' in files
            assert 'meta.xml' in files
            assert 'eml.xml' in files

            with zf.open('meta.xml') as meta_xml_file:
                meta_str = meta_xml_file.read().decode("utf-8")
                assert remove_pretty_print_xml(meta_str) == remove_pretty_print_xml(expected_meta_xml)

            with zf.open('occurrence.csv') as occ_file:
                df = pd.read_csv(occ_file, dtype='str')
                expected_df = occ_df.drop([1, 2])
                pd.testing.assert_frame_equal(df.drop(columns="id"), expected_df)

            zf.close()

    def test_delete_records_dwca_ext(self):
        """
        Test for record deletion in occurrence core and multimedia extension
        """
        occ_df = pd.DataFrame(data=[["1", "species1", "-30.0000", "144.0000"],
                                    ["2", "species2", "-28.0000", "115.0000"],
                                    ["3", "species3", "-36.0000", "144.30848"]],
                              columns=['occurrenceID', 'scientificName', 'decimalLatitude', 'decimalLongitude'])

        multimedia_df = pd.DataFrame(data=[["1", "https://image1.jpg", "image/jpeg", "StillImage"],
                                           ["2", "https://image2.jpg", "image/jpeg", "StillImage"],
                                           ["3", "https://image3.jpg", "image/jpeg", "StillImage"]],
                                     columns=["occurrenceID", "identifier", "format", "type"])

        dwca_ext_obj = make_dwca(occ_df, multimedia_df)

        delete_df = pd.DataFrame(data=[["2", "species2"],
                                       ["3", "species3"]],
                                 columns=['occurrenceID', 'scientificName'])

        delete_records = CsvFileType(files=delete_df, type='occurrence', keys=['occurrenceID'])

        output_obj = BytesIO()

        DwcaHandler.delete_records(dwca_file=dwca_ext_obj, records_to_delete=delete_records, output_dwca_path=output_obj)

        expected_meta_xml = make_meta_xml_str(occ_df, multimedia_df)

        with ZipFile(output_obj, 'r') as zf:
            files = zf.namelist()
            assert 'occurrence.csv' in files
            assert 'multimedia.csv' in files
            assert 'meta.xml' in files
            assert 'eml.xml' in files

            with zf.open('meta.xml') as meta_xml_file:
                meta_str = meta_xml_file.read().decode("utf-8")
                assert remove_pretty_print_xml(meta_str) == remove_pretty_print_xml(expected_meta_xml)

            with zf.open('occurrence.csv') as occ_file:
                df = pd.read_csv(occ_file, dtype='str')
                expected_df = occ_df.drop([1, 2])
                pd.testing.assert_frame_equal(df.drop(columns="id"), expected_df)

            with zf.open('multimedia.csv') as multimedia_file:
                multimedia_df_output = pd.read_csv(multimedia_file, dtype='str')
                expected_mult_df = multimedia_df.drop([1, 2])
                pd.testing.assert_frame_equal(multimedia_df_output.drop(columns="coreid"), expected_mult_df)

            zf.close()

