"""
Tools to convert data frames into Darwin Core Archive (DwCA) files.

See https://ipt.gbif.org/manual/en/ipt/2.6/dwca-guide for a guide to DwCAs.

A DwCA essentially contains tables, in the form of CSV files (either comma- or tab-separated).
A core file contains the main table, with one record to a row.
Extension files may contain additional tables, connected to the core file in a
1 (core) to many (extension) relationship, linked by an identifier in a one column of each file.
Note that this structure is considerably more restrictive than a relational database.

A meta-file contains the table descriptions and information about the CSV encoding and the
links between the files.
A particular function of the meta-file is that it can link columns to URIs that specify
the (usually Darwin Core) terms that each column contains.

"""
from collections import namedtuple
from dataclasses import dataclass, field
from typing import Optional
import logging
import pandas as pd
from functools import wraps

CoreOrExtType = namedtuple("CoreOrExtType", ["CORE", "EXTENSION"])(
    CORE="core",
    EXTENSION="extension"
)


@dataclass
class CSVEncoding:
    """The encoding used in a CSV file.

    The default encoding follows the general DwCA comma conventions.
    Comma-delimited, with a newline between each record, double quotes to enclose test
    and repeated double quotes to escape quotes.

    DwCA meta-files are XML files and may have specially escaped characters in them
    to describe the encoding.
    """
    csv_delimiter: str = field(default=',')
    csv_eol: str = field(default='\n')
    csv_text_enclosure: str = field(default='"')
    csv_escape_char: str = field(default='"')

    def __post_init__(self):
        self.csv_delimiter = self.__convert_values(self.csv_delimiter)
        self.csv_eol = self.__convert_values(self.csv_eol) if self.csv_eol != '' else '\n'

    def __convert_values(self, v):
        """Convert escaped character specifications into their actual counterparts.

        For example the literal "\t" becomes the tab character.

        :param v: The character string to convert
        :return: The actual character to use."""
        translate_table: dict = {'LF': '\r\n', '\\t': '\t', '\\n': '\n', '&quot;': '"'}
        return translate_table[v] if v in translate_table.keys() else v


@dataclass
class CsvFileType:
    """A description of a CSV file in a DwCA
    """
    files: list  # can accept more than one file
    type: str  # 'occurrence', 'taxon', 'event', multimedia,...
    keys: Optional[list] = None  # must be supplied for csv extensions to link extension records to core record
    # when creating dwca. for core other than occurrence, this neeeds to be supplied as key.
    # column keys lookup in core or extension for delete records
    associated_files_loc: Optional[str] = None  # in case there are associated media that need to be packaged in dwca
    csv_encoding: CSVEncoding = field(
        default_factory=lambda: CSVEncoding(csv_delimiter=",", csv_eol="\n", csv_text_enclosure='"',
                                            csv_escape_char='"'))
    # delimiter: Optional[str] = None
    # file delimiter type when reading the csv. if not supplied, the collectory setting delimiter is read in for the dr


@dataclass
class DataFrameType:
    df: pd.DataFrame
    type: str  # 'occurrence', 'taxon', 'event', multimedia,...
    keys: Optional[list] = None  # must be supplied for csv extensions to link extension records to core record
    # when creating dwca. for core other than occurrence, this neeeds to be supplied as key.
    # column keys lookup in core or extension for delete records
    associated_files_loc: Optional[str] = None  # in case there are associated media that need to be packaged in dwca


class Stat:
    """Record statistics for a DwCA"""
    start_record_count: int = 0
    current_record_count: int = 0
    updated_record_count: int = 0

    def __init__(self, records: int = 0):
        """
        Initialise the statistics.

        :param records: The intial number of records (0 by default)
        """
        self.start_record_count = records
        self.current_record_count = records

    def set_stat(self, new_count):
        """Set the current record count.

        :param new_count: The new current record count"""
        self.current_record_count = new_count

    def add_stat(self, new_count):
        """Add to the current record count.

        :param new_count: The amount to add to the current record count"""
        self.current_record_count += new_count

    def set_update_stat(self, update_count):
        """Set the updated record count.

         :param update_count: The new updated record count"""
        self.updated_record_count = update_count

    def add_update_stat(self, update_count):
        """Add to the updated record count.

        :param update_count: The amount to add to the updated record count"""
        self.updated_record_count += update_count

    def diff(self) -> int:
        """Get the different between the start record count and the current record count.

        :return: The count difference"""
        return (self.current_record_count - self.start_record_count) \
            if self.current_record_count > self.start_record_count \
            else (self.start_record_count - self.current_record_count)

    def get_stat(self) -> str:
        """Get the statistics as a formatted string.

        :return: A string describing the statistics"""
        return f"Start: {str(self.start_record_count)}, \
                 New: {str(self.current_record_count)}, \
                 Diff: {str(self.diff())} \
                 Updates: {str(self.updated_record_count)}"

    def __str__(self) -> str:
        """String representation

        :return: The statistics string"""
        return self.get_stat()


def record_diff_stat(func):
    """Record stats for dataframe content"""
    @wraps(func)
    def wrapper_function(self, record_content, content, *args, **kwargs):
        ret_value = func(self, record_content, content, *args, **kwargs)
        record_content.stat.set_stat(self.count_stat(ret_value))
        logging.debug("%s %s %s stats shows %s",
                      func.__name__, record_content.meta_info.core_or_ext_type,
                      record_content.meta_info.type.name, str(record_content.stat))
        return ret_value

    return wrapper_function


@dataclass
class Defaults:
    """
    A class to hold default properties for Dwca
    """
    csv_encoding: CSVEncoding = field(
        default_factory=lambda: CSVEncoding(csv_delimiter=",", csv_eol="\n", csv_text_enclosure='"',
                                            csv_escape_char='"'))
    eml_xml_filename: str = 'eml.xml'
    meta_xml_filename: str = 'meta.xml'
    # Translation csv encoding values
    translate_table: dict = field(init=False,
                                  default_factory=lambda: {'LF': '\r\n', '\\t': '\t', '\\n': '\n'})


# Imports at end of file to allow classes to be used
from dwcahandler.dwca.terms import Terms
from dwcahandler.dwca.dwca_meta import Element, MetaElementTypes, MetaElementInfo, MetaDwCA
from dwcahandler.dwca.eml import Eml
from dwcahandler.dwca.base_dwca import BaseDwca
from dwcahandler.dwca.core_dwca import Dwca, DfContent
from dwcahandler.dwca.dwca_factory import DwcaHandler

