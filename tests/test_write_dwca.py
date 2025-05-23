import io
from dwcahandler import DwcaHandler, ContentData, CoreOrExtType, MetaElementTypes
from zipfile import ZipFile
from pathlib import Path
import xml.etree.ElementTree as ET
import re
import pandas as pd
from tests import get_eml_content


def _get_namespace(element):
    """Get the namespace from a `{namespace}tag` formatted URI

    param: element
    :return: The namespace for the element
    """
    m = re.match("\\{.*\\}", element.tag)
    return m.group(0) if m else ''


occurrence_sample_file = "./input_files/occurrence/sample1/occurrence.txt"
multimedia_sample_file = "./input_files/occurrence/sample1/multimedia.txt"
sample_occ_df = pd.read_csv(occurrence_sample_file)
sample_multimedia_df = pd.read_csv(multimedia_sample_file)


class TestWriteDwca:
    """
    This checks that the dwca file that is produced has the proper meta xml schema and format and
    content is as expected
    """

    def test_generate_dwca_without_ext(self):
        """
        Test that generated dwca is valid with core occ data
        """
        core_csv = ContentData(data=[occurrence_sample_file], keys=['occurrenceID'],
                               type=MetaElementTypes.OCCURRENCE)
        p = Path("temp")
        p.mkdir(parents=True, exist_ok=True)
        dwca_output_path = str(Path(p / "dwca.zip").absolute())
        DwcaHandler.create_dwca(core_csv=core_csv, output_dwca=dwca_output_path,
                                eml_content=get_eml_content())
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
                core_node = root.find(f'{ns}{CoreOrExtType.CORE.value}')
                assert core_node is not None
                fields = core_node.findall(f'{ns}field')
                term_fields = [f.attrib.get('term') for f in fields]
                assert len(term_fields) == len(sample_occ_df.columns)
                for sample_col in sample_occ_df.columns:
                    assert any(sample_col in f for f in term_fields)
                core_file = core_node.find(f'{ns}files').find(f'{ns}location').text

            assert core_file
            with zf.open(core_file) as occ_file:
                df = pd.read_csv(occ_file)
                pd.testing.assert_frame_equal(df, sample_occ_df)

            zf.close()

    def test_generate_dwca_with_ext(self):
        """
        Test that generated dwca is valid with core occ and multimedia data
        """
        core_csv = ContentData(data=[occurrence_sample_file], keys=['occurrenceID'],
                               type=MetaElementTypes.OCCURRENCE)
        ext_csv = ContentData(data=[multimedia_sample_file], keys=['occurrenceID'],
                              type=MetaElementTypes.MULTIMEDIA)
        p = Path("temp")
        p.mkdir(parents=True, exist_ok=True)
        dwca_output_path = str(Path(p / "dwca_with_ext.zip").absolute())
        DwcaHandler.create_dwca(core_csv=core_csv, ext_csv_list=[ext_csv], output_dwca=dwca_output_path,
                                eml_content=get_eml_content())
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
                core_node = root.find(f'{ns}{CoreOrExtType.CORE.value}')
                assert core_node is not None
                fields = core_node.findall(f'{ns}field')
                term_fields = [f.attrib.get('term') for f in fields]
                assert len(term_fields) == len(sample_occ_df.columns)
                for sample_col in sample_occ_df.columns:
                    assert any(sample_col in f for f in term_fields)
                core_file = core_node.find(f'{ns}files').find(f'{ns}location').text

                ext_node = root.find(f'{ns}{CoreOrExtType.EXTENSION.value}')
                assert ext_node is not None
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
                pd.testing.assert_frame_equal(df, sample_occ_df)

            with zf.open(ext_file) as image_file:
                df = pd.read_csv(image_file)
                pd.testing.assert_frame_equal(df, sample_multimedia_df)

            zf.close()

    def test_generate_dwca_in_memory(self):
        """
        Test that generated dwca in memory is valid with core occ data
        """

        occ_df = pd.DataFrame(data=[["1", "species1"],
                                    ["2", "species2"],
                                    ["3", "species3"]],
                              columns=['catalogNumber', 'scientificName'])

        core_csv = ContentData(data=occ_df,
                               type=MetaElementTypes.OCCURRENCE,
                               keys=['catalogNumber'])

        dwca_output = io.BytesIO()

        DwcaHandler.create_dwca(core_csv=core_csv, output_dwca=dwca_output,
                                eml_content=get_eml_content())

        with ZipFile(dwca_output, 'r') as zf:
            files = zf.namelist()
            assert 'meta.xml' in files
            assert 'eml.xml' in files
            core_file = ""
            with zf.open('meta.xml') as meta_xml_file:
                tree = ET.parse(meta_xml_file)
                root = tree.getroot()
                ns = _get_namespace(root)
                assert ns == "{http://rs.tdwg.org/dwc/text/}"
                core_node = root.find(f'{ns}{CoreOrExtType.CORE.value}')
                assert core_node is not None
                fields = core_node.findall(f'{ns}field')
                term_fields = [f.attrib.get('term') for f in fields]
                assert len(term_fields) == len(occ_df.columns)
                for sample_col in occ_df.columns:
                    assert any(sample_col in f for f in term_fields)
                core_file = core_node.find(f'{ns}files').find(f'{ns}location').text

            assert core_file
            with zf.open(core_file) as occ_file:
                df = pd.read_csv(occ_file, dtype='str')
                pd.testing.assert_frame_equal(df, occ_df)

            zf.close()
