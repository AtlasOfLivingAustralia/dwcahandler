
import pandas as pd
from dwcahandler import CsvFileType, DwcaHandler, MetaElementTypes
from pathlib import Path
from io import BytesIO
from tests import get_eml_content, get_xml_from_file
from zipfile import ZipFile
import glob
import os


def check_output(output_obj: BytesIO, test_files_folder: str, check_core_id: bool = False):

    test_files_list = glob.glob(os.path.join(test_files_folder, "*.txt"))
    expected_meta_xml_path = os.path.join(test_files_folder, "meta.xml")

    with ZipFile(output_obj, 'r') as zf:
        files = zf.namelist()

        for test_csv in test_files_list:
            assert Path(test_csv).name in files

        assert 'meta.xml' in files
        assert 'eml.xml' in files

        with zf.open('meta.xml') as meta_xml_file:
            meta_str = meta_xml_file.read().decode("utf-8")
            expected_meta_xml = get_xml_from_file(str(expected_meta_xml_path))
            assert meta_str == expected_meta_xml

        for txt_file in files:
            with zf.open(txt_file) as txt_file:
                for test_file in test_files_list:
                    if txt_file.name == Path(test_file).name:
                        actual_df = pd.read_csv(txt_file, dtype='str')
                        expected_df = pd.read_csv(test_file, dtype='str')
                        if not check_core_id:
                            pd.testing.assert_frame_equal(actual_df, expected_df)
                        else:
                            core_id_list = ["id", "coreid"]
                            assert any(found := [i for i in core_id_list if i in actual_df.columns.to_list()])
                            actual_df = actual_df.drop(columns=[found[0]])
                            for col in expected_df.columns:
                                expected_df = expected_df[~expected_df[col].str.contains('ERROR')]
                            pd.testing.assert_frame_equal(actual_df, expected_df)

        zf.close()


class TestCreateDwca:

    def test_create_occurrence_dwca_occurrence(self):
        test_files_folder = "./input_files/occurrence/sample1"

        core_csv = CsvFileType(files=[f"{test_files_folder}/occurrence.txt"], keys=['occurrenceID'],
                               type=MetaElementTypes.OCCURRENCE)
        ext1_csv = CsvFileType(files=[f"{test_files_folder}/multimedia.txt"],
                               type=MetaElementTypes.MULTIMEDIA)

        output_obj = BytesIO()

        DwcaHandler.create_dwca(core_csv=core_csv, ext_csv_list=[ext1_csv], output_dwca=output_obj,
                                eml_content=get_eml_content())

        assert output_obj

        check_output(output_obj, test_files_folder)

    def test_create_occurrence_dwca_occurrence_multiple_keys(self):
        test_files_folder = "./input_files/occurrence/sample2"

        core_csv = CsvFileType(files=[f"{test_files_folder}/occurrence.txt"],
                               keys=['institutionCode', 'collectionCode', 'catalogNumber'],
                               type=MetaElementTypes.OCCURRENCE)
        ext1_csv = CsvFileType(files=[f"{test_files_folder}/multimedia.txt"],
                               type=MetaElementTypes.MULTIMEDIA)

        output_obj = BytesIO()

        DwcaHandler.create_dwca(core_csv=core_csv, ext_csv_list=[ext1_csv], output_dwca=output_obj,
                                eml_content=get_eml_content())

        assert output_obj

        check_output(output_obj, test_files_folder, check_core_id=True)

    def test_create_occurrence_dwca_occurrence_extra_multimedia_records(self):
        test_files_folder = "./input_files/occurrence/sample3"

        core_csv = CsvFileType(files=[f"{test_files_folder}/occurrence.txt"],
                               keys=['institutionCode', 'collectionCode', 'catalogNumber'],
                               type=MetaElementTypes.OCCURRENCE)
        ext1_csv = CsvFileType(files=[f"{test_files_folder}/multimedia.txt"],
                               type=MetaElementTypes.MULTIMEDIA)

        output_obj = BytesIO()

        DwcaHandler.create_dwca(core_csv=core_csv, ext_csv_list=[ext1_csv], output_dwca=output_obj,
                                eml_content=get_eml_content())

        assert output_obj

        check_output(output_obj, test_files_folder, check_core_id=True)

    def test_create_event_dwca_sample1(self):

        test_files_folder = "./input_files/event/cameratrap-sample1"

        core_csv = CsvFileType(files=[f"{test_files_folder}/event.txt"], keys=['eventID'],
                               type=MetaElementTypes.EVENT)
        ext1_csv = CsvFileType(files=[f"{test_files_folder}/occurrence.txt"], keys=['occurrenceID'],
                               type=MetaElementTypes.OCCURRENCE)
        ext2_csv = CsvFileType(files=[f"{test_files_folder}/measurement_or_fact.txt"],
                               type=MetaElementTypes.MEASUREMENT_OR_FACT)

        output_obj = BytesIO()

        DwcaHandler.create_dwca(core_csv=core_csv, ext_csv_list=[ext1_csv, ext2_csv], output_dwca=output_obj,
                                eml_content=get_eml_content())

        assert output_obj

        check_output(output_obj, test_files_folder)

    def test_create_event_dwca_sample2(self):

        test_files_folder = "./input_files/event/cameratrap-sample2"

        core_csv = CsvFileType(files=[f"{test_files_folder}/event.txt"], keys=['eventID'],
                               type=MetaElementTypes.EVENT)
        ext1_csv = CsvFileType(files=[f"{test_files_folder}/occurrence.txt"], keys=['occurrenceID'],
                               type=MetaElementTypes.OCCURRENCE)
        ext2_csv = CsvFileType(files=[f"{test_files_folder}/extended_measurement_or_fact.txt"],
                               type=MetaElementTypes.EXTENDED_MEASUREMENT_OR_FACT)

        output_obj = BytesIO()

        DwcaHandler.create_dwca(core_csv=core_csv, ext_csv_list=[ext1_csv, ext2_csv], output_dwca=output_obj,
                                eml_content=get_eml_content())

        assert output_obj

        check_output(output_obj, test_files_folder)

    def test_create_occurrence_dwca_occurrence_without_ext(self):
        test_files_folder = "./input_files/occurrence/sample4"

        core_csv = CsvFileType(files=[f"{test_files_folder}/occurrence.txt"],
                               type=MetaElementTypes.OCCURRENCE)

        output_obj = BytesIO()

        DwcaHandler.create_dwca(core_csv=core_csv, ext_csv_list=[], output_dwca=output_obj,
                                eml_content=get_eml_content())

        assert output_obj

        check_output(output_obj, test_files_folder)
