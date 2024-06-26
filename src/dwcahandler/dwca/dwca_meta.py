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
from urllib.parse import urlparse
from dataclasses import dataclass, field, asdict
from typing import ClassVar
from typing import Optional
from dwcahandler.dwca import CSVEncoding, CoreOrExtType, Terms


@dataclass
class Element:
    """A mapping of a name to a URI, giving the class of a row type"""
    name: str
    row_type_ns: str


# noinspection SpellCheckingInspection
@dataclass
class MetaElementTypes:
    """Named row types that map common DwCA row types onto URIs"""
    occurrence: ClassVar[Element] = \
        Element("occurrence", "http://rs.tdwg.org/dwc/terms/Occurrence")
    multimedia: ClassVar[Element] = \
        Element("multimedia", "http://rs.gbif.org/terms/1.0/Multimedia")
    organism: ClassVar[Element] = \
        Element("organism", "http://rs.tdwg.org/dwc/terms/Organism")
    materialsample: ClassVar[Element] = \
        Element("materialsample", "http://rs.tdwg.org/dwc/terms/MaterialSample")
    location: ClassVar[Element] = \
        Element("location", "http://rs.tdwg.org/dwc/terms/Location")
    event: ClassVar[Element] = \
        Element("event", "http://rs.tdwg.org/dwc/terms/Event")
    taxon: ClassVar[Element] = \
        Element("taxon", "http://rs.tdwg.org/dwc/terms/Taxon")
    measurementorfact: ClassVar[Element] = \
        Element("measurementorfact", "http://rs.tdwg.org/dwc/terms/MeasurementOrFact")
    resourcerelationship: ClassVar[Element] = \
        Element("resourcerelationship", "http://rs.tdwg.org/dwc/terms/ResourceRelationship")
    chronometricage: ClassVar[Element] = \
        Element("chronometricage", "http://rs.tdwg.org/dwc/terms/ChronometricAge")

    @staticmethod
    def get_element(name: str):
        """Find a row type by name

        :param name: The row name
        :return: The element corresponding to the row name
        """
        try:
            return MetaElementTypes.__dict__[name.lower()]
        except KeyError:
            return MetaElementTypes.get_element_by_row_type(name)

    @staticmethod
    def get_element_by_row_type(row_type: str):
        """Find a row type by URI

        :param row_type: The row type URI
        :return: The corresponding element
        """
        for elm in asdict(MetaElementTypes()).values():
            if elm['row_type_ns'] == row_type:
                return MetaElementTypes.get_element(elm['name'])

        # For custom namespace
        return Element(MetaElementTypes.extract_term(row_type), row_type)

    @staticmethod
    def extract_term(term_string):
        """Find a term name based on a term or a URI

        :param term_string: The term or URI
        :return: The term name
        """
        path_entity = urlparse(term_string)
        path_str = path_entity.path
        match = re.search(r'/([^/]*)$', path_str)
        if match is not None:
            return match[1]

        return term_string


@dataclass
class MetaElementInfo:
    """A description of a core or extension file containing whether
    the file is core or extension, the row type, the CSV encoding and
    the local file name for the table."""
    core_or_ext_type: CoreOrExtType
    type: Element
    csv_encoding: CSVEncoding
    ignore_header_lines: str = '1'
    charset_encoding: str = 'UTF-8'
    file_name: str = field(default='')

    def __post_init__(self):
        if not self.file_name:
            self.file_name = f'{self.type.name}.csv'


@dataclass
class Field:
    """A field for a CSV file in a DwCA, mapping the CSV column onto a name or URI, with an optional
    default and vocabulary."""
    index: int
    field_name: str
    term: Optional[str] = None
    default: Optional[str] = None
    vocabulary: Optional[str] = None


@dataclass
class MetaElementAttributes:
    """A meta-description of a DwCA file"""
    meta_element_type: MetaElementInfo
    fields: list[Field] = field(default_factory=list)


@dataclass
class MetaDwCA:
    """Complete Metadata for a DwCA including dataset metadata and schema information"""
    eml_xml_filename: str = field(default='eml.xml')
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
        file_name = node_elm.find(f'{ns}files').find(f'{ns}location').text
        meta_element_info = MetaElementInfo(
            core_or_ext_type=core_or_ext_type,
            type=MetaElementTypes.get_element_by_row_type(node_elm.attrib['rowType']),
            csv_encoding=CSVEncoding(
                csv_delimiter=node_elm.attrib['fieldsTerminatedBy'],
                csv_eol=node_elm.attrib['linesTerminatedBy'],
                csv_text_enclosure=node_elm.attrib['fieldsEnclosedBy']
                if node_elm.attrib['fieldsEnclosedBy'] != '' else '"'),
            ignore_header_lines=node_elm.attrib['ignoreHeaderLines'],
            charset_encoding=node_elm.attrib['encoding'],
            file_name=file_name)
        # set first field with index 0 if it's not present in list of fields
        if fields[0].attrib['index'] != '0':
            if CoreOrExtType.CORE == core_or_ext_type:
                field_list = [Field(index=0, field_name="id")]
            else:
                field_list = [Field(index=0, field_name="coreid")]
        else:
            field_list = []
        field_list.extend(
            [Field(index=int(extract_field_attr_value(f, 'index'))
                   if extract_field_attr_value(f, 'index') else None,
                   field_name=MetaElementTypes.extract_term(extract_field_attr_value(f, 'term')),
                   term=extract_field_attr_value(f, 'term'),
                   default=extract_field_attr_value(f, 'default'),
                   vocabulary=extract_field_attr_value(f, 'vocabulary'))
             for i, f in enumerate(fields)])
        meta_element_attributes = \
            MetaElementAttributes(meta_element_type=meta_element_info, fields=field_list)
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
        node_elm = root.find(f'{ns}{CoreOrExtType.CORE}')
        self.meta_elements = [self.__extract_meta_info(ns, node_elm, CoreOrExtType.CORE)]
        self.meta_elements.extend(
            [self.__extract_meta_info(ns, ne, CoreOrExtType.EXTENSION)
             for ne in root.findall(f'{ns}{CoreOrExtType.EXTENSION}')])

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

    def map_headers(self, headers: list[str], start_index: int = -1) -> list[Field]:
        """Map header column names onto a list of fields.

        Column names are mapped onto fields based on name, URI or qualified name

        :param headers: The header list
        :param start_index: The start index for the field index, as an offset from the header index
        :return: The corresponding field list
        """
        field_list: list[Field] = []
        index = 0
        for i, col in enumerate(headers):
            col_name = self.__remove_prefix(col)
            if i == 0:
                index = start_index if start_index > -1 else i
            else:
                index += 1
            field_elm = Field(index=index, field_name=col_name, term=self.__get_terms(col_name))
            field_list.append(field_elm)
        return field_list

    def _extract_meta_element(self, file_name):
        for _, elm in enumerate(self.meta_elements):
            if elm.meta_element_type.file_name == file_name:
                return elm
        return None

    def update_meta_element(self, meta_element_info: MetaElementInfo, fields: list[str]):
        """Replace or append meta information (based on file name)

        :param meta_element_info: The info
        :param fields: The field list
        """
        replace = False
        for i, elm in enumerate(self.meta_elements):
            if elm.meta_element_type.file_name == meta_element_info.file_name:
                field_list: list[Field] = self.map_headers(fields, elm.fields[0].index)
                self.meta_elements[i] = \
                    MetaElementAttributes(meta_element_type=meta_element_info, fields=field_list)
                replace = True

        if not replace:
            field_list: list[Field] = self.map_headers(fields)
            self.meta_elements.append(
                MetaElementAttributes(meta_element_type=meta_element_info, fields=field_list))

    def _build_meta_xml(self, meta_elem_attrib: MetaElementAttributes):
        """Build a core/extension row for `meta.xml`

        :param meta_elem_attrib: The meta information for the row
        """
        elem = ET.SubElement(self.dwca_meta, meta_elem_attrib.meta_element_type.core_or_ext_type)
        elem.attrib['encoding'] = meta_elem_attrib.meta_element_type.charset_encoding
        elem.attrib['rowType'] = meta_elem_attrib.meta_element_type.type.row_type_ns
        elem.attrib['fieldsTerminatedBy'] = meta_elem_attrib.meta_element_type.csv_encoding.csv_delimiter
        elem.attrib['linesTerminatedBy'] = \
            "\\r\\n" if (meta_elem_attrib.meta_element_type.csv_encoding.csv_eol in ['\r\n', '\n', '\\n']) \
            else meta_elem_attrib.meta_element_type.csv_encoding.csv_eol
        elem.attrib['fieldsEnclosedBy'] = meta_elem_attrib.meta_element_type.csv_encoding.csv_text_enclosure
        elem.attrib['ignoreHeaderLines'] = meta_elem_attrib.meta_element_type.ignore_header_lines

        files = ET.SubElement(elem, 'files')
        location = ET.SubElement(files, 'location')
        location.text = meta_elem_attrib.meta_element_type.file_name
        id_field = ET.SubElement(elem, 'id') \
            if meta_elem_attrib.meta_element_type.core_or_ext_type == 'core' \
            else ET.SubElement(elem, 'coreid')
        id_field.attrib['index'] = '0'

        for _, f in enumerate(meta_elem_attrib.fields):
            if f.field_name not in ('id', 'coreid'):
                field_elem = ET.SubElement(elem, "field")
                field_elem.attrib['index'] = str(f.index)
                if f.term:
                    field_elem.attrib['term'] = f.term

    def create(self):
        """Create a `meta.xml` file for this meta-infomation
        """
        self.dwca_meta.attrib['xmlns'] = 'http://rs.tdwg.org/dwc/text/'
        self.dwca_meta.attrib['metadata'] = self.eml_xml_filename

        for elm in self.meta_elements:
            self._build_meta_xml(meta_elem_attrib=elm)

    def __str__(self):
        return minidom.parseString(ET.tostring(self.dwca_meta)).toprettyxml(indent="  ")
