import pandas as pd
from zipfile import ZipFile
import zipfile
from io import BytesIO
import csv
from dwcahandler import Eml
from xml.dom.minidom import parseString


def get_eml_content():
    eml = Eml(dataset_name='Sample Dataset',
              description='A dataset sample',
              license='sample license',
              citation='sample citation',
              rights='sample rights')
    return eml.build_eml_xml()


def make_fields(columns: list, term_uri: str, field_start: int = 0, core_id: str = None):
    fields = ""
    idx_start = 0
    if field_start != -1:
       fields = core_id if core_id else ""
       idx_start = field_start if field_start != -2 else 0


    for idx, col in enumerate(columns):
        if not (col in ["id", "coreid"]):
            dwc_term_uri = "http://rs.tdwg.org/dwc/terms" if col == 'occurrenceID' else term_uri
            fields = fields + '\n' + f'<field index="{str(idx + idx_start)}" term="{dwc_term_uri}/{col}"/>'

    return fields


def make_ext_str(ext_columns: list, term_uri: str, field_start: int, use_col_idx_as_core_id: int):
    ext_meta_str = ''
    fields = make_fields(ext_columns, term_uri, field_start, f'<coreid index="{use_col_idx_as_core_id}" />')
    if fields:
        ext_meta_str = f'''
<extension encoding="UTF-8" rowType="http://rs.gbif.org/terms/1.0/Multimedia" fieldsTerminatedBy="," linesTerminatedBy="\\r\\n" fieldsEnclosedBy="&quot;" ignoreHeaderLines="1">
    <files>
      <location>multimedia.csv</location>
    </files>
    {fields}
  </extension>
'''
    return ext_meta_str


def make_meta_xml_str(core_df: pd.DataFrame, ext_df: pd.DataFrame = None, use_col_idx_as_core_id: int = None) -> str:
    """
    Create a meta xml string based on the core and extension dataframe
    This meta xml is based on occurrence core and optional multimedia ext
    :param: core_df dataframe for occurrence core
            ext_df dataframe for multimedia extension
    :return: str
    """
    core_columns = core_df.columns.to_list()
    field_start = use_col_idx_as_core_id #1 if any(x for x in core_columns if x in ["id", "coreid"]) else use_col_idx_as_core_id
    id_idx = use_col_idx_as_core_id if use_col_idx_as_core_id >= 0 else 0
    fields = make_fields(core_columns, "http://rs.tdwg.org/dwc/terms", field_start,
                         f'<id index="{id_idx}" />')
    ext_str = make_ext_str(ext_df.columns.to_list(), "http://purl.org/dc/terms",
                           field_start, id_idx) \
        if isinstance(ext_df, pd.DataFrame) else ''
    meta_xml_str = f'''<?xml version="1.0" ?>
<archive xmlns="http://rs.tdwg.org/dwc/text/" metadata="eml.xml">
    <core encoding="UTF-8" rowType="http://rs.tdwg.org/dwc/terms/Occurrence" fieldsTerminatedBy="," linesTerminatedBy="\\r\\n" fieldsEnclosedBy="&quot;" ignoreHeaderLines="1">
        <files>
            <location>occurrence.csv</location>
        </files>
        {fields}
    </core>{ext_str}
</archive>'''
    return meta_xml_str


def make_dwca(core_content: pd.DataFrame, ext_mult_content: pd.DataFrame = None, use_col_idx_as_core_id: int = -1) -> BytesIO:
    """
    Create a darwin core archive in memory for testing
    :param: core_df dataframe for occurrence core
            ext_df dataframe for multimedia extension
    :return: BytesIO
    """
    zip_buffer = BytesIO()
    meta_xml_str = make_meta_xml_str(core_content, ext_mult_content, use_col_idx_as_core_id)
    content = core_content.copy(deep=True)

    with ZipFile(file=zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
        zf.writestr(zinfo_or_arcname='occurrence.csv',
                    data=content.to_csv(header=True, quoting=csv.QUOTE_MINIMAL, index=False))
        if isinstance(ext_mult_content, pd.DataFrame):
            multimedia_content = ext_mult_content.copy(deep=True)

            zf.writestr(zinfo_or_arcname='multimedia.csv',
                        data=multimedia_content.to_csv(header=True, quoting=csv.QUOTE_MINIMAL, index=False))
        zf.writestr(zinfo_or_arcname='eml.xml',
                    data=get_eml_content())
        zf.writestr(zinfo_or_arcname='meta.xml', data=meta_xml_str)
        zf.close()
    return zip_buffer


def remove_pretty_print_xml(input_xml):
    _dom = parseString(input_xml)
    output_xml = ''.join([line.strip() for line in _dom.toxml().splitlines()])
    _dom.unlink()
    return output_xml


from dwcahandler import MetaDwCA
from io import BytesIO

def get_xml_from_file(expected_file: str):
    dwca_meta = MetaDwCA()
    dwca_meta.read_meta_file (meta_file=expected_file)
    dwca_meta.create()
    expected_str = str(dwca_meta)
    return expected_str
