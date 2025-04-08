"""
Schema for DwCAs
----------------

DwCAs contain a `meta.xml` file that describes the nature of the table files in the
archive, in terms of row type, encoding and columns.
These classes model the schema information required by a DwCA.
"""
import xml.etree.ElementTree as ET
from xml.dom import minidom
import re
from dataclasses import dataclass, field
from typing import Optional
from dwcahandler.dwca import CSVEncoding, CoreOrExtType, Terms, Defaults
from enum import Enum


DwcClassRowTypes = Terms.get_class_row_types()

MetaElementTypes = Enum("MetaElementTypes", dict(DwcClassRowTypes))


def get_meta_class_row_type(row_type_uri: str):
    """
    Find a row type by URI

    :param row_type_uri: The row type URI
    :return: The corresponding element
    """
    for name, member in MetaElementTypes.__members__.items():
        if member.value == row_type_uri:
            return member
    return None


@dataclass
class MetaElementInfo:
    """A description of a core or extension file containing whether
    the file is core or extension, the row type, the CSV encoding and
    the local file name for the table."""
    core_or_ext_type: CoreOrExtType
    type: MetaElementTypes
    csv_encoding: CSVEncoding
    ignore_header_lines: str = '1'
    charset_encoding: str = 'UTF-8'
    file_name: str = field(default='')

    def __post_init__(self):
        if not self.file_name:
            self.file_name = f'{self.type.name.lower()}.txt'


@dataclass
class Field:
    """A field for a CSV file in a DwCA, mapping the CSV column onto a name or URI, with an optional
    default and vocabulary."""
    index: str = None
    field_name: str = None
    term: Optional[str] = None
    default: Optional[str] = None
    vocabulary: Optional[str] = None


@dataclass
class MetaElementAttributes:
    """A meta-description of a DwCA file"""
    meta_element_type: MetaElementInfo
    core_id: Field
    fields: list[Field] = field(default_factory=list)


@dataclass
class MetaDwCA:
    """Complete Metadata for a DwCA including dataset metadata and schema information"""
    eml_xml_filename: str = field(default=Defaults.eml_xml_filename)
    dwca_meta: ET.Element = field(init=False)
    meta_elements: list[MetaElementAttributes] = field(default_factory=list, init=False)

    def __post_init__(self):
        self.terms_df = Terms().terms_df

        # initialise own instance of meta content
        self.dwca_meta = ET.Element('archive')

    def __extract_meta_info(self, ns, node_elm, core_or_ext_type):

        def extract_field_attr_value(field_elm, attrib):
            return field_elm.attrib.get(attrib) if field_elm.attrib.get(attrib) else None

        fields = node_elm.findall(f'{ns}field')
        id_field = []
        if core_or_ext_type == CoreOrExtType.CORE:
            id_field = node_elm.findall(f'{ns}{Defaults.MetaDefaultFields.ID}')
        else:
            id_field = node_elm.findall(f'{ns}{Defaults.MetaDefaultFields.CORE_ID}')
        file_name = node_elm.find(f'{ns}files').find(f'{ns}location').text
        file_name = file_name.strip()
        meta_element_info = MetaElementInfo(
            core_or_ext_type=core_or_ext_type,
            type=get_meta_class_row_type(node_elm.attrib['rowType']),
            csv_encoding=CSVEncoding(
                csv_delimiter=node_elm.attrib['fieldsTerminatedBy'],
                csv_eol=node_elm.attrib['linesTerminatedBy'],
                csv_text_enclosure=node_elm.attrib['fieldsEnclosedBy']
                if node_elm.attrib['fieldsEnclosedBy'] != '' else '"'),
            ignore_header_lines=node_elm.attrib['ignoreHeaderLines'],
            charset_encoding=node_elm.attrib['encoding'],
            file_name=file_name)

        field_list = []
        field_list.extend(
            [Field(index=extract_field_attr_value(f, 'index')
                   if extract_field_attr_value(f, 'index') else None,
                   field_name=Terms.extract_term(extract_field_attr_value(f, 'term')),
                   term=extract_field_attr_value(f, 'term'),
                   default=extract_field_attr_value(f, 'default'),
                   vocabulary=extract_field_attr_value(f, 'vocabulary'))
             for i, f in enumerate(fields)])
        index_number = id_field[0].attrib["index"] if len(id_field) > 0 else None
        meta_element_attributes = \
            MetaElementAttributes(meta_element_type=meta_element_info, fields=field_list,
                                  core_id=Field(index=index_number) if index_number else None)
        return meta_element_attributes

    def _get_namespace(self, element):
        """Get the namespace from a `{namespace}tag` formatted URI

        :param element" The element
        "return: The namespace for the element
        """
        m = re.match("\\{.*\\}", element.tag)
        return m.group(0) if m else ''

    def read_meta_file(self, meta_file):
        """Read the `meta.xml` file in a DwCA into this information

        :param meta_file: The path to the meta file
        """
        tree = ET.parse(meta_file)
        root = tree.getroot()
        ns = self._get_namespace(root)
        node_elm = root.find(f"{ns}{CoreOrExtType.CORE.value}")
        self.meta_elements = [self.__extract_meta_info(ns, node_elm, CoreOrExtType.CORE)]
        self.meta_elements.extend(
            [self.__extract_meta_info(ns, ne, CoreOrExtType.EXTENSION)
             for ne in root.findall(f"{ns}{CoreOrExtType.EXTENSION.value}")])

    def remove_meta_elements(self, exts_to_remove):
        """Remove extension files from the metadata

        :param exts_to_remove: Extension files to remove
        """
        self.meta_elements = [meta_elem for meta_elem in self.meta_elements if
                              meta_elem.meta_element_type.file_name not in exts_to_remove]

    def __remove_prefix(self, col_name):
        """Remove common column name namespace prefixes from a name"""
        prefixes = ['dcterms:', 'dcterms_', 'ggbn:', 'ggbn_']
        for prefix in prefixes:
            col_name = col_name.removeprefix(prefix)
        return col_name

    def __get_terms(self, field_elm):
        # Some terms from dwca contain strings like dcterms:
        col_name = self.__remove_prefix(field_elm)
        return col_name if len(self.terms_df[self.terms_df['term'].str.lower() == col_name.lower()]) <= 0 \
            else self.terms_df[self.terms_df['term'].str.lower() == col_name.lower()]['uri'].values[0]

    def map_headers(self, headers: list[str], index_field: str = None) -> (list[Field], Optional[Field]):
        """Map header column names onto a list of fields.

        Column names are mapped onto fields based on name, URI or qualified name

        :param headers: The header list
        :param index_field: The id or coreid if any
        :return: The corresponding field list
        """
        field_list: list[Field] = []
        id_index = None
        for i, col in enumerate(headers):
            col_name = self.__remove_prefix(col)
            field_elm = Field(index=str(i), field_name=col_name, term=self.__get_terms(col_name))
            if index_field and self.__remove_prefix(index_field) == col_name:
                id_index = field_elm
            field_list.append(field_elm)
        return field_list, id_index

    def update_meta_element(self, meta_element_info: MetaElementInfo, fields: list[str], index_field: str = None):
        """Replace or append meta information (based on file name)

        :param index_field: The field that is also form part of the id/coreid
        :param meta_element_info: The meta element info
        :param fields: The field list
        """
        replace = False
        for i, elm in enumerate(self.meta_elements):
            if elm.meta_element_type.file_name == meta_element_info.file_name:
                (field_list, core_id) = self.map_headers(fields, index_field)
                if not core_id:
                    core_id = elm.core_id
                self.meta_elements[i] = \
                    MetaElementAttributes(meta_element_type=meta_element_info, fields=field_list, core_id=core_id)
                replace = True

        if not replace:
            (field_list, core_id) = self.map_headers(fields, index_field)
            self.meta_elements.append(
                MetaElementAttributes(meta_element_type=meta_element_info, fields=field_list, core_id=core_id))

    def _build_meta_xml(self, meta_elem_attrib: MetaElementAttributes):
        """Build a core/extension row for `meta.xml`

        :param meta_elem_attrib: The meta information for the row
        """
        elem = ET.SubElement(self.dwca_meta, meta_elem_attrib.meta_element_type.core_or_ext_type.value)
        elem.attrib['encoding'] = meta_elem_attrib.meta_element_type.charset_encoding
        elem.attrib['rowType'] = meta_elem_attrib.meta_element_type.type.value
        elem.attrib['fieldsTerminatedBy'] = meta_elem_attrib.meta_element_type.csv_encoding.csv_delimiter
        elem.attrib['linesTerminatedBy'] = \
            "\\r\\n" if (meta_elem_attrib.meta_element_type.csv_encoding.csv_eol in ['\r\n', '\n', '\\n']) \
            else meta_elem_attrib.meta_element_type.csv_encoding.csv_eol
        elem.attrib['fieldsEnclosedBy'] = meta_elem_attrib.meta_element_type.csv_encoding.csv_text_enclosure
        elem.attrib['ignoreHeaderLines'] = meta_elem_attrib.meta_element_type.ignore_header_lines

        files = ET.SubElement(elem, 'files')
        location = ET.SubElement(files, 'location')
        location.text = meta_elem_attrib.meta_element_type.file_name
        if meta_elem_attrib.core_id:
            id_field = ET.SubElement(elem, Defaults.MetaDefaultFields.ID) \
                if meta_elem_attrib.meta_element_type.core_or_ext_type == CoreOrExtType.CORE \
                else ET.SubElement(elem, Defaults.MetaDefaultFields.CORE_ID)
            id_field.attrib['index'] = meta_elem_attrib.core_id.index

        for _, f in enumerate(meta_elem_attrib.fields):
            if f.field_name not in list(Defaults.MetaDefaultFields):
                field_elem = ET.SubElement(elem, "field")
                if f.index is not None:
                    field_elem.attrib['index'] = f.index
                if f.term:
                    field_elem.attrib['term'] = f.term
                if f.vocabulary:
                    field_elem.attrib['vocabulary'] = f.vocabulary
                if f.default:
                    field_elem.attrib['default'] = f.default

    def create(self):
        """Create a `meta.xml` file for this meta-information
        """
        self.dwca_meta.attrib['xmlns'] = 'http://rs.tdwg.org/dwc/text/'
        self.dwca_meta.attrib['metadata'] = self.eml_xml_filename

        for elm in self.meta_elements:
            self._build_meta_xml(meta_elem_attrib=elm)

    def __str__(self):
        return minidom.parseString(ET.tostring(self.dwca_meta)).toprettyxml(indent="  ")
