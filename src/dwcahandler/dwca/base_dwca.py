"""
Module contains the Darwin Core Base class

"""

from __future__ import annotations
from abc import ABCMeta, abstractmethod
from typing import Union
from io import BytesIO
from dwcahandler.dwca import CoreOrExtType, CsvFileType, MetaElementTypes
from dwcahandler.dwca.eml import Eml


class BaseDwca(metaclass=ABCMeta):
    """An abstract DwCA that provides basic operations"""

    @abstractmethod
    def extract_csv_content(self, csv_info: CsvFileType, core_ext_type: CoreOrExtType,
                            build_coreid_for_ext: bool = False):
        """Get the content from a single file in the DwCA

        :param csv_info: The CSV file to extract
        :param core_ext_type: Is this a core or extension CSV file
        :param build_coreid_for_ext: indicator to add id and core id
        """
        pass

    @abstractmethod
    def generate_eml(self, eml_content: str = ""):
        """Generate an EML metadata file for this archive.

        An Ecological Markup Language file is an XML file that contains dataset
        metadata for research data.
        This method generates a suitable `eml.xml` file for inclusion in the DwCA.

        See https://eml.ecoinformatics.org/
        """
        pass

    @abstractmethod
    def generate_meta(self):
        """Generate a meta-file for this archive.

        The meta-file, `meta.xml` contains a table schema and mapping of columns onto
        terms that can be used to interpret the content of the archive.
        """
        pass

    @abstractmethod
    def write_dwca(self, output_dwca_path: Union[str | BytesIO]):
        """Write the content of the DwCA to a directory.

        Writes all CSV files, as well as a meta-file and EML file for the archive.

        :param output_dwca_path: The path to write to
        """
        pass

    @abstractmethod
    def extract_dwca(self, exclude_ext_files: list = None):
        """Extract content of dwca into memory of the dwca class

        :param exclude_ext_files:
        """
        pass

    @abstractmethod
    def merge_contents(self, delta_dwca: BaseDwca, extension_sync: bool, match_by_filename: bool=False):
        """Construct a new DwCA by merging the contents of a delta DwCA with this archive.

        :param delta_dwca: The delta to merge
        :param extension_sync: Merge extensions
        :param match_by_filename: Match the dwca and delta content by also filenames if supplied,
                this is extra condition in case if there are more than 1 content with same class type in a dwca
                in a rare circumstances
        """
        pass

    @abstractmethod
    def set_keys(self, keys: dict):
        pass

    @abstractmethod
    def convert_associated_media_to_extension(self):
        pass

    @abstractmethod
    def delete_records(self, records_to_delete: CsvFileType):
        pass

    @abstractmethod
    def validate_content(self, content_to_validate: list[MetaElementTypes] = None, error_file: str = None):
        pass

    @abstractmethod
    def get_content(self, class_type: MetaElementTypes = None, name_space: str = None):
        pass

    @abstractmethod
    def add_multimedia_info_to_content(self, multimedia_content):
        """
        Add format or type if not provided for multimedia ext
        """
        pass

    def fill_additional_info(self):
        """
        Adds extra info based on the information in the content, mainly used by ingestion process
        """
        contents = self.get_content(class_type=MetaElementTypes.MULTIMEDIA)
        for multimedia_content, _ in contents:
            self.add_multimedia_info_to_content(multimedia_content)

    def delete_records_in_dwca(self, records_to_delete: CsvFileType, output_dwca: Union[str, BytesIO]):
        """Delete records in dwca if the key records are defined in CsvFileType

        :param records_to_delete: A CsvFileType that containing the text file of the record keys,
                                  the key names of the records and MetaElementType type class of the dwca
                                  where the records need to be removed
        :param output_dwca: output dwca path where the result of the dwca is writen to or the output dwca in memory
        """
        self.extract_dwca()
        self.delete_records(records_to_delete)
        self.generate_eml()
        self.generate_meta()
        self.write_dwca(output_dwca)

    def create_dwca(self, core_csv: CsvFileType, output_dwca: Union[str, BytesIO],
                    ext_csv_list: list[CsvFileType] = None, validate_content: bool = True,
                    eml_content: Union[str, Eml] = ''):
        """Create a dwca given the contents of core and extensions and eml content

        :param core_csv: CsvFileType containing the files, class types and keys to form the core of the dwca
        :param output_dwca: the resulting path of the dwca or the dwca in memory
        :param ext_csv_list: list of CsvFileTypes containing the files, class types and keys to form the
                              extensions of the dwca if supplied
        :param validate_content: whether to validate the contents
        :param eml_content: eml content in string or a filled Eml object
        """
        if ext_csv_list is None:
            ext_csv_list = []

        self.extract_csv_content(csv_info=core_csv, core_ext_type=CoreOrExtType.CORE,
                                 build_coreid_for_ext=True if len(ext_csv_list) > 0 else False)

        # Only validate core content
        if validate_content and not self.validate_content():
            raise SystemExit(Exception("Some validations error found. Dwca is not created."))

        # if multimedia files is supplied, do not attempt to convert associated media to multimedia
        if not any(ext.type == MetaElementTypes.MULTIMEDIA for ext in ext_csv_list):
            image_ext = self.convert_associated_media_to_extension()
            if image_ext:
                ext_csv_list.append(image_ext)

        for ext in ext_csv_list:
            self.extract_csv_content(ext, CoreOrExtType.EXTENSION, True)

        self.fill_additional_info()
        self.generate_eml(eml_content)
        self.generate_meta()
        self.write_dwca(output_dwca)

    # https://peps.python.org/pep-0484/#forward-references
    def merge_dwca(self, delta_dwca: BaseDwca, output_dwca: Union[str, BytesIO], keys_lookup: dict = None,
                   extension_sync=False, validate_delta: bool = True):
        """Merging another dwca to bring in the new records and update the existing records

        :param delta_dwca: delta dwca that contains the updated or new records
        :param output_dwca: output dwca containing the path to the physical file and the output of dwca writen in memory
        :param keys_lookup: keys to lookup merging with delta_dwca to update content
        :param extension_sync:
        :param validate_delta:
        """
        self.extract_dwca()
        delta_dwca.extract_dwca()
        self.set_keys(keys_lookup)
        delta_dwca.set_keys(keys_lookup)
        if validate_delta and not delta_dwca.validate_content():
            raise SystemExit(Exception("Some validations error found in the delta dwca. Dwca is not merged."))
        self.merge_contents(delta_dwca, extension_sync)
        self.fill_additional_info()
        self.generate_eml()
        self.generate_meta()
        self.write_dwca(output_dwca)

    def validate_dwca(self, content_keys: dict, error_file: str):
        """Validate dwca to check if content has unique keys. By default, validates the core content.
           If additional checks required in another content, supply it as content_keys

        :param content_keys: a dictionary of class type and the key
                             for eg. {MetaElementTypes.OCCURRENCE, "occurrenceId"}
        :param error_file: optional error_file for the errored data
        """
        self.extract_dwca()
        set_keys = self.set_keys(content_keys)
        return self.validate_content(content_to_validate=set_keys, error_file=error_file)

    def validate_file(self, csv: CsvFileType, error_file: str):
        """Validate the text file

        :param csv: CsvFileType to pass the csv, key and type
        :param error_file: optional error_file for the errored data
        """
        self.extract_csv_content(csv, CoreOrExtType.CORE)
        return self.validate_content(error_file=error_file)
