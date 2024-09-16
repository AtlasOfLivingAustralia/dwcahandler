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


def make_meta_xml_str(columns: list):
    """
    Create a meta xml string based on the columns
    :param columns:
    :return:
    """
    fields = ''
    for idx, col in enumerate(columns):
        field = f'<field index="{idx + 1}" term="http://rs.tdwg.org/dwc/terms/{col}"/>'
        fields = field if idx == 0 else fields + '\n' + field
    meta_xml_str = f'''<?xml version="1.0" ?>
<archive xmlns="http://rs.tdwg.org/dwc/text/" metadata="eml.xml">
    <core encoding="UTF-8" rowType="http://rs.tdwg.org/dwc/terms/Occurrence" fieldsTerminatedBy="," linesTerminatedBy="\\r\\n" fieldsEnclosedBy="&quot;" ignoreHeaderLines="1">
        <files>
            <location>occurrence.csv</location>
        </files>
        <id index="0"/>
        {fields}
    </core>
</archive>'''
    return meta_xml_str


def make_dwca(content: pd.DataFrame):
    zip_buffer = BytesIO()
    meta_xml_str = make_meta_xml_str(content.columns.to_list())
    content.insert(loc=0, column='id', value=content['occurrenceID'])
    with ZipFile(file=zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
        zf.writestr(zinfo_or_arcname='occurrence.csv',
                    data=content.to_csv(header=True, quoting=csv.QUOTE_MINIMAL, index=False))
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
