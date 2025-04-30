# flake8: noqa
"""
Tools to convert data frame or text files into Darwin Core Archive (DwCA) file.

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
from __future__ import annotations

import io
import logging
from collections import namedtuple
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
from typing import Optional, Union
import pandas as pd


class CoreOrExtType(Enum):
    CORE = "core"
    EXTENSION = "extension"

# Default keys for content when creating dwca
DefaultKeys = namedtuple("DefaultKeys", ["EVENT", "OCCURRENCE", "MULTIMEDIA"])(
    EVENT = "eventID",
    OCCURRENCE = "occurrenceID",
    MULTIMEDIA = "identifier"
)

class ValidationError(Enum):
    EMPTY_KEYS = "EMPTY_KEYS"
    DUPLICATE_KEYS = "DUPLICATE_KEYS"
    DUPLICATE_COLUMNS = "DUPLICATE_COLUMNS"
    UNNAMED_COLUMNS = "UNNAMED_COLUMNS"


def get_error_report() -> pd.DataFrame:
    return pd.DataFrame(pd.DataFrame(columns=["Content", "Message", "Error", "Row"]))

def get_keys(class_type: MetaElementTypes, override_content_keys: dict[[MetaElementTypes, list]] = None):
    """
    # If override_content_keys not supplied, return the default keys based on content type
    :param class_type: class_type of content
    :param override_content_keys: given content keys
    :return: the list of keys for the content
    """
    if override_content_keys:
        for content_type, keys in override_content_keys.items():
            if class_type == content_type and keys and len(keys) > 0:
                return keys
    defaults = DefaultKeys._asdict()
    return [defaults[class_type.name]] if class_type.name in defaults.keys() else []

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
    def wrapper_function(self, *args, **kwargs):
        params = list(kwargs.keys())
        if len(params) >= 1:
            record_content = kwargs[params[0]]
            ret_value = func(self, *args, **kwargs)
            record_content.stat.set_stat(self.count_stat(ret_value))
            logging.debug("%s %s %s stats shows %s",
                          func.__name__, record_content.meta_info.core_or_ext_type,
                          record_content.meta_info.type.name, str(record_content.stat))
            return ret_value

        ret_value = func(self, *args, **kwargs)
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
    MetaDefaultFields: namedtuple = namedtuple("MetaDefaultFields", ["ID", "CORE_ID"])(
                                        ID="id",
                                        CORE_ID="coreid"
                                    )



# Imports at end of file to allow classes to be used
from dwcahandler.dwca.terms import Terms, NsPrefix
from dwcahandler.dwca.dwca_meta import (MetaElementTypes, MetaElementInfo, MetaDwCA,
                                        MetaElementAttributes, get_meta_class_row_type)
@dataclass
class ContentData:
    """A class describing the content data used for core and extension.
       Use this class to define the core content and extension content to build a DwCA (see README on usage)
    """
    data: Union[list[str], pd.DataFrame, io.TextIOWrapper]  # can accept more than one files, dataframe or file pointer
    type: MetaElementTypes # Enumerated types from the class row type.
    keys: Optional[list] = None # keys that uniquely identify a record in the content
    associated_files_loc: Optional[str] = None  # provide a folder path containing the embedded images.
                                # Embedded images file name must be supplied as associatedMedia in the content
    csv_encoding: CSVEncoding = field(
        default_factory=lambda: CSVEncoding(csv_delimiter=",", csv_eol="\n", csv_text_enclosure='"',
                                            csv_escape_char='"'))

    def check_for_empty(self, include_keys = True):
        if self.data and len(self.data) > 0 and \
                self.type and isinstance(self.type, MetaElementTypes) and \
                (not include_keys or include_keys and self.keys and len(self.keys) > 0):
            return True
        return False

    def add_data(self, other_csv_file_type: ContentData):
        if self.type and self.type == other_csv_file_type.type:
            if isinstance(self.data, pd.DataFrame) and isinstance(other_csv_file_type.data, pd.DataFrame):
                self.data = pd.concat([self.data, other_csv_file_type.data], ignore_index=False)
                return True
            elif isinstance(self.data, list) and isinstance(other_csv_file_type.data, list):
                self.data.append(other_csv_file_type.data)
                return True
        elif not self.type:
            self.data = other_csv_file_type.data
            self.type = other_csv_file_type.type
        return False

from dwcahandler.dwca.eml import Eml
from dwcahandler.dwca.base_dwca import BaseDwca
from dwcahandler.dwca.core_dwca import Dwca, DfContent
from dwcahandler.dwca.dwca_factory import DwcaHandler

