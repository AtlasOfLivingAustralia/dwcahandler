
import pandas as pd
from zipfile import ZipFile
from tests import make_meta_xml_str, remove_pretty_print_xml
from tests import make_dwca
from dwcahandler import DwcaHandler, MetaElementTypes
from io import BytesIO
from numpy import nan


class TestMergeDwcaContent:

    def test_merge_core_records_with_id(self):
        """
        Test for core record merging (update existing and add new rows)
        """
        occ_df = pd.DataFrame(data=[["1", "species1", "-30.0000", "144.0000"],
                                    ["2", "species2", "-28.0000", "115.0000"],
                                    ["3", "species3", "-36.0000", "144.30848"]],
                              columns=['occurrenceID', 'scientificName', 'decimalLatitude', 'decimalLongitude'])

        dwca_obj = make_dwca(core_content=occ_df, use_col_idx_as_core_id=0)

        delta_occ_df = pd.DataFrame(data=[["3", "species3", "-40.0000", "144.0000"],
                                          ["4", "species4", "-10.0000", "144.0000"],
                                          ["5", "species5", "-20.0000", "145.0000"],
                                          ["6", "species6", "-30.0000", "146.3048"]],
                                    columns=['occurrenceID', 'scientificName', 'decimalLatitude', 'decimalLongitude'])

        delta_dwca_obj = make_dwca(core_content=delta_occ_df, use_col_idx_as_core_id=0)

        output_obj = BytesIO()

        keys_lookup: dict = dict()
        keys_lookup[MetaElementTypes.OCCURRENCE] = ['occurrenceID']

        DwcaHandler.merge_dwca(dwca_file=dwca_obj, delta_dwca_file=delta_dwca_obj,
                               output_dwca=output_obj,
                               keys_lookup=keys_lookup)

        expected_meta_xml = make_meta_xml_str(core_df=occ_df, use_col_idx_as_core_id=0)

        expected_occ_df = pd.DataFrame(data=[["1", "species1", "-30.0000", "144.0000"],
                                             ["2", "species2", "-28.0000", "115.0000"],
                                             ["3", "species3", "-40.0000", "144.0000"],
                                             ["4", "species4", "-10.0000", "144.0000"],
                                             ["5", "species5", "-20.0000", "145.0000"],
                                             ["6", "species6", "-30.0000", "146.3048"]],
                                       columns=['occurrenceID', 'scientificName', 'decimalLatitude', 'decimalLongitude'])

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
                pd.testing.assert_frame_equal(df, expected_occ_df)

            zf.close()

    def test_merge_core_records_without_id(self):
        """
        Test for core record merging (update existing and add new rows)
        """
        occ_df = pd.DataFrame(data=[["1", "species1", "-30.0000", "144.0000"],
                                    ["2", "species2", "-28.0000", "115.0000"],
                                    ["3", "species3", "-36.0000", "144.30848"]],
                              columns=['occurrenceID', 'scientificName', 'decimalLatitude', 'decimalLongitude'])

        dwca_obj = make_dwca(core_content=occ_df, use_col_idx_as_core_id=-1)

        delta_occ_df = pd.DataFrame(data=[["3", "species3", "-40.0000", "144.0000"],
                                          ["4", "species4", "-10.0000", "144.0000"],
                                          ["5", "species5", "-20.0000", "145.0000"],
                                          ["6", "species6", "-30.0000", "146.3048"]],
                                    columns=['occurrenceID', 'scientificName', 'decimalLatitude', 'decimalLongitude'])

        delta_dwca_obj = make_dwca(core_content=delta_occ_df, use_col_idx_as_core_id=-1)

        output_obj = BytesIO()

        keys_lookup: dict = dict()
        keys_lookup[MetaElementTypes.OCCURRENCE] = ['occurrenceID']

        DwcaHandler.merge_dwca(dwca_file=dwca_obj, delta_dwca_file=delta_dwca_obj,
                               output_dwca=output_obj,
                               keys_lookup=keys_lookup)

        expected_meta_xml = make_meta_xml_str(core_df=occ_df, use_col_idx_as_core_id=-1)

        expected_occ_df = pd.DataFrame(data=[["1", "species1", "-30.0000", "144.0000"],
                                             ["2", "species2", "-28.0000", "115.0000"],
                                             ["3", "species3", "-40.0000", "144.0000"],
                                             ["4", "species4", "-10.0000", "144.0000"],
                                             ["5", "species5", "-20.0000", "145.0000"],
                                             ["6", "species6", "-30.0000", "146.3048"]],
                                       columns=['occurrenceID', 'scientificName', 'decimalLatitude', 'decimalLongitude'])

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
                pd.testing.assert_frame_equal(df, expected_occ_df)

            zf.close()

    def test_merge_core_records_with_separate_id(self):
        """
        Test for core record merging (update existing and add new rows)
        """
        occ_df = pd.DataFrame(data=[["1", "occ1", "species1", "-30.0000", "144.0000"],
                                    ["2", "occ2", "species2", "-28.0000", "115.0000"],
                                    ["3", "occ3", "species3", "-36.0000", "144.30848"]],
                              columns=["id", "occurrenceID", 'scientificName', 'decimalLatitude', 'decimalLongitude'])

        dwca_obj = make_dwca(core_content=occ_df, use_col_idx_as_core_id=-2)

        delta_occ_df = pd.DataFrame(data=[["3", "occ3", "species3", "-40.0000", "144.0000"],
                                          ["4", "occ4", "species4", "-10.0000", "144.0000"],
                                          ["5", "occ5", "species5", "-20.0000", "145.0000"],
                                          ["6", "occ6", "species6", "-30.0000", "146.3048"]],
                                    columns=["id", "occurrenceID", "scientificName", "decimalLatitude", "decimalLongitude"])

        delta_dwca_obj = make_dwca(core_content=delta_occ_df, use_col_idx_as_core_id=-2)

        output_obj = BytesIO()

        keys_lookup: dict = dict()
        keys_lookup[MetaElementTypes.OCCURRENCE] = ['occurrenceID']

        DwcaHandler.merge_dwca(dwca_file=dwca_obj, delta_dwca_file=delta_dwca_obj,
                               output_dwca=output_obj,
                               keys_lookup=keys_lookup)

        expected_meta_xml = make_meta_xml_str(core_df=occ_df, use_col_idx_as_core_id=-2)

        expected_occ_df = pd.DataFrame(data=[["1", "occ1", "species1", "-30.0000", "144.0000"],
                                             ["2", "occ2", "species2", "-28.0000", "115.0000"],
                                             ["3", "occ3", "species3", "-40.0000", "144.0000"],
                                             ["4", "occ4", "species4", "-10.0000", "144.0000"],
                                             ["5", "occ5", "species5", "-20.0000", "145.0000"],
                                             ["6", "occ6", "species6", "-30.0000", "146.3048"]],
                                       columns=["id", "occurrenceID", "scientificName", "decimalLatitude", "decimalLongitude"])

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
                pd.testing.assert_frame_equal(df, expected_occ_df)

            zf.close()

    def test_merge_core_and_ext_records_with_id(self):
        """
        Test for core and extension record merging (update existing and add new rows, columns)
        Occurrence, multimedia and meta xml output is merged as expected
        """
        occ_df = pd.DataFrame(data=[["1", "species1", "-30.0000", "144.0000"],
                                    ["2", "species2", "-28.0000", "115.0000"],
                                    ["3", "species3", "-36.0000", "144.30848"]],
                              columns=['occurrenceID', 'scientificName', 'decimalLatitude', 'decimalLongitude'])

        multimedia_df = pd.DataFrame(data=[["1", "https://image1.jpg", "image/jpeg", "StillImage"],
                                           ["2", "https://image2.jpg", "image/jpeg", "StillImage"],
                                           ["3", "https://image3.jpg", "image/jpeg", "StillImage"]],
                                     columns=["occurrenceID", "identifier", "format", "type"])

        dwca_ext_obj = make_dwca(core_content=occ_df, ext_mult_content=multimedia_df, use_col_idx_as_core_id=0)

        delta_occ_df = pd.DataFrame(data=[["3", "species3", "-40.0000", "144.0000", "Observation"],
                                          ["4", "species4", "-10.0000", "144.0000", "Observation"],
                                          ["5", "species5", "-20.0000", "145.0000", nan],
                                          ["6", "species6", "-30.0000", "146.3048", nan]],
                                    columns=["occurrenceID", "scientificName", "decimalLatitude", "decimalLongitude",
                                             "basisOfRecord"])

        delta_multimedia_df = pd.DataFrame(data=[["3", "https://new-image3.webp", "image/webp", "StillImage", "RightsHolder3"],
                                                 ["4", "https://image4.webp", "image/webp", "StillImage", nan],
                                                 ["5", "https://image5.webp", "image/webp", "StillImage", "RightsHolder5"],
                                                 ["6", "https://image6.webp", "image/webp", "StillImage", "RightsHolder6"]],
                                           columns=["occurrenceID", "identifier", "format", "type", "rightsHolder"])

        delta_dwca_ext_obj = make_dwca(core_content=delta_occ_df, ext_mult_content=delta_multimedia_df, use_col_idx_as_core_id=0)

        output_obj = BytesIO()

        keys_lookup: dict = dict()
        keys_lookup[MetaElementTypes.OCCURRENCE] = ['occurrenceID']

        DwcaHandler.merge_dwca(dwca_file=dwca_ext_obj, delta_dwca_file=delta_dwca_ext_obj,
                               output_dwca=output_obj,
                               keys_lookup=keys_lookup)

        expected_occ_df = pd.DataFrame(data=[["1", "species1", "-30.0000", "144.0000", nan],
                                             ["2", "species2", "-28.0000", "115.0000", nan],
                                             ["3", "species3", "-40.0000", "144.0000", "Observation"],
                                             ["4", "species4", "-10.0000", "144.0000", "Observation"],
                                             ["5", "species5", "-20.0000", "145.0000", nan],
                                             ["6", "species6", "-30.0000", "146.3048", nan]],
                                       columns=['occurrenceID', 'scientificName', 'decimalLatitude', 'decimalLongitude',
                                                'basisOfRecord'])

        expected_multimedia_df = pd.DataFrame(data=[["1", "https://image1.jpg", "image/jpeg", "StillImage", nan],
                                                    ["2", "https://image2.jpg", "image/jpeg", "StillImage", nan],
                                                    ["3", "https://new-image3.webp", "image/webp", "StillImage", "RightsHolder3"],
                                                    ["4", "https://image4.webp", "image/webp", "StillImage", nan],
                                                    ["5", "https://image5.webp", "image/webp", "StillImage", "RightsHolder5"],
                                                    ["6", "https://image6.webp", "image/webp", "StillImage", "RightsHolder6"]],
                                              columns=["occurrenceID", "identifier", "format", "type", "rightsHolder"])

        expected_meta_xml = make_meta_xml_str(core_df=expected_occ_df, ext_df=expected_multimedia_df, use_col_idx_as_core_id=0)

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
                df_output = pd.read_csv(occ_file, dtype='str')
                pd.testing.assert_frame_equal(df_output, expected_occ_df)

            with zf.open('multimedia.csv') as multimedia_file:
                multimedia_df_output = pd.read_csv(multimedia_file, dtype='str')
                pd.testing.assert_frame_equal(multimedia_df_output, expected_multimedia_df)

            zf.close()

    def test_merge_core_and_ext_records_with_separate_id(self):
        """
        Test for core and extension record merging (update existing and add new rows, columns)
        Occurrence, multimedia and meta xml output is merged as expected
        """
        occ_df = pd.DataFrame(data=[["1", "occ1", "species1", "-30.0000", "144.0000"],
                                    ["2", "occ2", "species2", "-28.0000", "115.0000"],
                                    ["3", "occ3", "species3", "-36.0000", "144.30848"]],
                              columns=["id", "occurrenceID", "scientificName", "decimalLatitude", "decimalLongitude"])

        multimedia_df = pd.DataFrame(data=[["1", "https://image1.jpg", "image/jpeg", "StillImage"],
                                           ["2", "https://image2.jpg", "image/jpeg", "StillImage"],
                                           ["3", "https://image3.jpg", "image/jpeg", "StillImage"]],
                                     columns=["id", "identifier", "format", "type"])

        dwca_ext_obj = make_dwca(core_content=occ_df, ext_mult_content=multimedia_df, use_col_idx_as_core_id=0)

        delta_occ_df = pd.DataFrame(data=[["3", "occ3", "species3", "-40.0000", "144.0000", "Observation"],
                                          ["4", "occ4", "species4", "-10.0000", "144.0000", "Observation"],
                                          ["5", "occ5", "species5", "-20.0000", "145.0000", nan],
                                          ["6", "occ6", "species6", "-30.0000", "146.3048", nan]],
                                    columns=["id", "occurrenceID", "scientificName", "decimalLatitude", "decimalLongitude",
                                             "basisOfRecord"])

        delta_multimedia_df = pd.DataFrame(data=[["3", "https://new-image3.webp", "image/webp", "StillImage", "RightsHolder3"],
                                                 ["4", "https://image4.webp", "image/webp", "StillImage", nan],
                                                 ["5", "https://image5.webp", "image/webp", "StillImage", "RightsHolder5"],
                                                 ["6", "https://image6.webp", "image/webp", "StillImage", "RightsHolder6"]],
                                           columns=["id", "identifier", "format", "type", "rightsHolder"])

        delta_dwca_ext_obj = make_dwca(core_content=delta_occ_df, ext_mult_content=delta_multimedia_df, use_col_idx_as_core_id=0)

        output_obj = BytesIO()

        keys_lookup: dict = dict()
        keys_lookup[MetaElementTypes.OCCURRENCE] = ['occurrenceID']

        DwcaHandler.merge_dwca(dwca_file=dwca_ext_obj, delta_dwca_file=delta_dwca_ext_obj,
                               output_dwca=output_obj,
                               keys_lookup=keys_lookup)

        expected_occ_df = pd.DataFrame(data=[["1", "occ1", "species1", "-30.0000", "144.0000", nan],
                                             ["2", "occ2", "species2", "-28.0000", "115.0000", nan],
                                             ["3", "occ3", "species3", "-40.0000", "144.0000", "Observation"],
                                             ["4", "occ4", "species4", "-10.0000", "144.0000", "Observation"],
                                             ["5", "occ5", "species5", "-20.0000", "145.0000", nan],
                                             ["6", "occ6", "species6", "-30.0000", "146.3048", nan]],
                                       columns=["id", "occurrenceID", "scientificName", "decimalLatitude", "decimalLongitude",
                                                "basisOfRecord"])

        expected_multimedia_df = pd.DataFrame(data=[["1", "https://image1.jpg", "image/jpeg", "StillImage", nan],
                                                    ["2", "https://image2.jpg", "image/jpeg", "StillImage", nan],
                                                    ["3", "https://new-image3.webp", "image/webp", "StillImage", "RightsHolder3"],
                                                    ["4", "https://image4.webp", "image/webp", "StillImage", nan],
                                                    ["5", "https://image5.webp", "image/webp", "StillImage", "RightsHolder5"],
                                                    ["6", "https://image6.webp", "image/webp", "StillImage", "RightsHolder6"]],
                                              columns=["coreid", "identifier", "format", "type", "rightsHolder"])

        expected_meta_xml = make_meta_xml_str(core_df=expected_occ_df, ext_df=expected_multimedia_df, use_col_idx_as_core_id=0)

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
                df_output = pd.read_csv(occ_file, dtype='str')
                pd.testing.assert_frame_equal(df_output, expected_occ_df)

            with zf.open('multimedia.csv') as multimedia_file:
                multimedia_df_output = pd.read_csv(multimedia_file, dtype='str')
                pd.testing.assert_frame_equal(multimedia_df_output, expected_multimedia_df)

            zf.close()
