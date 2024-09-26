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


def make_fields(columns: list, term_uri: str):
    fields = ''
    for idx, col in enumerate(columns):
        dwc_term_uri = "http://rs.tdwg.org/dwc/terms" if col == 'occurrenceID' else term_uri
        field = f'<field index="{idx + 1}" term="{dwc_term_uri}/{col}"/>'
        fields = field if idx == 0 else fields + '\n' + field
    return fields


def make_ext_str(ext_columns: list, term_uri: str):
    ext_meta_str = ''
    fields = make_fields(ext_columns, term_uri)
    if fields:
        ext_meta_str = f'''
<extension encoding="UTF-8" rowType="http://rs.gbif.org/terms/1.0/Multimedia" fieldsTerminatedBy="," linesTerminatedBy="\\r\\n" fieldsEnclosedBy="&quot;" ignoreHeaderLines="1">
    <files>
      <location>multimedia.csv</location>
    </files>
    <coreid index="0"/>
    {fields}
  </extension>
'''
    return ext_meta_str


def make_meta_xml_str(core_df: pd.DataFrame, ext_df: pd.DataFrame = None) -> str:
    """
    Create a meta xml string based on the core and extension dataframe
    This meta xml is based on occurrence core and optional multimedia ext
    :param: core_df dataframe for occurrence core
            ext_df dataframe for multimedia extension
    :return: str
    """
    core_columns = core_df.columns.to_list()
    fields = make_fields(core_columns, "http://rs.tdwg.org/dwc/terms")
    ext_str = make_ext_str(ext_df.columns.to_list(), "http://purl.org/dc/terms") \
        if isinstance(ext_df, pd.DataFrame) else ''
    meta_xml_str = f'''<?xml version="1.0" ?>
<archive xmlns="http://rs.tdwg.org/dwc/text/" metadata="eml.xml">
    <core encoding="UTF-8" rowType="http://rs.tdwg.org/dwc/terms/Occurrence" fieldsTerminatedBy="," linesTerminatedBy="\\r\\n" fieldsEnclosedBy="&quot;" ignoreHeaderLines="1">
        <files>
            <location>occurrence.csv</location>
        </files>
        <id index="0"/>
        {fields}
    </core>{ext_str}
</archive>'''
    return meta_xml_str


def make_dwca(core_content: pd.DataFrame, ext_mult_content: pd.DataFrame = None) -> BytesIO:
    """
    Create a darwin core archive in memory for testing
    :param: core_df dataframe for occurrence core
            ext_df dataframe for multimedia extension
    :return: BytesIO
    """
    zip_buffer = BytesIO()
    meta_xml_str = make_meta_xml_str(core_content, ext_mult_content)
    content = core_content.copy(deep=True)
    content.insert(loc=0, column='id', value=content['occurrenceID'])
    with ZipFile(file=zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
        zf.writestr(zinfo_or_arcname='occurrence.csv',
                    data=content.to_csv(header=True, quoting=csv.QUOTE_MINIMAL, index=False))
        if isinstance(ext_mult_content, pd.DataFrame):
            multimedia_content = ext_mult_content.copy(deep=True)
            multimedia_content.insert(loc=0, column='coreid', value=content['occurrenceID'])
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
