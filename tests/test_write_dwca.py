from dwcahandler import DwcaHandler, CsvFileType, CoreOrExtType, Eml
from zipfile import ZipFile
from pathlib import Path
import xml.etree.ElementTree as ET
import re
import pandas as pd


def _get_namespace(element):
    """Get the namespace from a `{namespace}tag` formatted URI

    param: element
    "return: The namespace for the element
    """
    m = re.match("\\{.*\\}", element.tag)
    return m.group(0) if m else ''


def _get_eml_content():
    return Eml(dataset_name='Sample Dataset',
               description='A dataset sample',
               license='sample license',
               citation='sample citation',
               rights='sample rights')


occurrence_sample_file = "./input_files/sample/occurrence.csv"
multimedia_sample_file = "./input_files/sample/multimedia.csv"
sample_occ_df = pd.read_csv(occurrence_sample_file)
sample_multimedia_df = pd.read_csv(multimedia_sample_file)


class TestWriteDwca:
    """
    Test for terms
    """

    def test_generate_dwca_without_ext(self):
        """
        Test that generated dwca is valid with core occ data
        """
        core_csv = CsvFileType(files=["./input_files/sample/occurrence.csv"], keys=['occurrenceID'],
                               type='occurrence')
        p = Path("temp")
        p.mkdir(parents=True, exist_ok=True)
        dwca_output_path = str(Path(p / "dwca.zip").absolute())
        DwcaHandler.create_dwca(core_csv=core_csv, output_dwca_path=dwca_output_path,
                                eml_content=_get_eml_content())
        with ZipFile(dwca_output_path, 'r') as zf:
            files = zf.namelist()
            assert 'meta.xml' in files
            assert 'eml.xml' in files
            core_file = ""
            with zf.open('meta.xml') as meta_xml_file:
                tree = ET.parse(meta_xml_file)
                root = tree.getroot()
                ns = _get_namespace(root)
                assert ns == "{http://rs.tdwg.org/dwc/text/}"
                core_node = root.find(f'{ns}{CoreOrExtType.CORE}')
                assert core_node
                fields = core_node.findall(f'{ns}field')
                term_fields = [f.attrib.get('term') for f in fields]
                assert len(term_fields) == len(sample_occ_df.columns)
                for sample_col in sample_occ_df.columns:
                    assert any(sample_col in f for f in term_fields)
                core_file = core_node.find(f'{ns}files').find(f'{ns}location').text

            assert core_file
            with zf.open(core_file) as occ_file:
                df = pd.read_csv(occ_file)
                pd.testing.assert_frame_equal(df.drop(columns=['id']), sample_occ_df)

            zf.close()

    def test_generate_dwca_with_ext(self):
        """
        Test that generated dwca is valid with core occ and multimedia data
        """
        core_csv = CsvFileType(files=["./input_files/sample/occurrence.csv"], keys=['occurrenceID'],
                               type='occurrence')
        ext_csv = CsvFileType(files=["./input_files/sample/multimedia.csv"], keys=['occurrenceID'],
                              type='multimedia')
        p = Path("temp")
        p.mkdir(parents=True, exist_ok=True)
        dwca_output_path = str(Path(p / "dwca_with_ext.zip").absolute())
        DwcaHandler.create_dwca(core_csv=core_csv, ext_csv_list=[ext_csv], output_dwca_path=dwca_output_path,
                                eml_content=_get_eml_content())
        with ZipFile(dwca_output_path, 'r') as zf:
            files = zf.namelist()
            assert 'meta.xml' in files
            assert 'eml.xml' in files
            core_file = ""
            with zf.open('meta.xml') as meta_xml_file:
                tree = ET.parse(meta_xml_file)
                root = tree.getroot()
                ns = _get_namespace(root)
                assert ns == "{http://rs.tdwg.org/dwc/text/}"
                core_node = root.find(f'{ns}{CoreOrExtType.CORE}')
                assert core_node
                fields = core_node.findall(f'{ns}field')
                term_fields = [f.attrib.get('term') for f in fields]
                assert len(term_fields) == len(sample_occ_df.columns)
                for sample_col in sample_occ_df.columns:
                    assert any(sample_col in f for f in term_fields)
                core_file = core_node.find(f'{ns}files').find(f'{ns}location').text

                ext_node = root.find(f'{ns}{CoreOrExtType.EXTENSION}')
                assert ext_node
                fields = ext_node.findall(f'{ns}field')
                term_fields = [f.attrib.get('term') for f in fields]
                assert len(term_fields) == len(sample_multimedia_df.columns)
                for sample_m_col in sample_multimedia_df.columns:
                    assert any(sample_m_col in f for f in term_fields)
                ext_file = ext_node.find(f'{ns}files').find(f'{ns}location').text

            assert core_file
            assert ext_file

            with zf.open(core_file) as occ_file:
                df = pd.read_csv(occ_file)
                assert 'id' in df.columns
                pd.testing.assert_frame_equal(df.drop(columns=['id']), sample_occ_df)

            with zf.open(ext_file) as image_file:
                df = pd.read_csv(image_file)
                assert 'coreid' in df.columns
                pd.testing.assert_frame_equal(df.drop(columns=['coreid']), sample_multimedia_df)

            zf.close()
