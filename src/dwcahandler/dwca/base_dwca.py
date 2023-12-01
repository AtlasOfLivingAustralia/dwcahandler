from __future__ import annotations
from abc import ABCMeta, abstractmethod
from . import CoreOrExtType, CsvFileType, DataFrameType
from typing import Union

class BaseDwca(metaclass=ABCMeta):
    """An abstract DwCA that provides basic operations"""

    @abstractmethod
    def _extract_csv_content(self, csv_info: CsvFileType, core_ext_type: CoreOrExtType):
        """Get the content from a single file in the DwCA

        :param csv_info: The CSV file to extract
        :param core_ext_type: Is this a core or extension CSV file
        """
        pass

    @abstractmethod
    def _generate_eml(self, eml_content: str = ""):
        """Generate an EML metadata file for this archive.

        An Ecological Markup Language file is an XML file that contains dataset
        metadata for research data.
        This method generates a suitable `eml.xml` file for inclusion in the DwCA.

        See https://eml.ecoinformatics.org/
        """
        pass

    @abstractmethod
    def _generate_meta(self):
        """Generate a meta-file for this archive.

        The meta-file, `meta.xml` contains a table schema and mapping of columns onto
        terms that can be used to interpret the content of the archive.
        """
        pass

    @abstractmethod
    def _write_dwca(self, output_dwca_path: str):
        """Write the content of the DwCA to a directory.

        Writes all CSV files, as well as a meta-file and EML file for the archive.

        :param output_dwca_path: The path to write to
        """
        pass

    @abstractmethod
    def _extract_dwca(self, exclude_ext_files: list = []):
        pass

    @abstractmethod
    def _merge_contents(self, delta_dwca, extension_sync: bool, regen_ids: bool):
        """Construct a new DwCA by merging the contents of a delta DwCA with this archive.

        :param delta_dwca: The delta to merge
        :param extension_sync: Merge extensions
        :param regen_ids: Regenerate link identifiers between the core and extension files.
        """
        pass

    @abstractmethod
    def _set_keys(self, keys: dict):
        pass

    @abstractmethod
    def _convert_associated_media_to_extension(self):
        pass

    @abstractmethod
    def _merge_df_dwc_columns(self):
        pass

    def _delete_records(self, records_to_delete: CsvFileType):
        pass

    @abstractmethod
    def _validate_content(self, content_type_to_validate: list[str] = None, error_file: str = None):
        pass

    def remove_extensions(self, exclude_ext_files: list, output_dwca_path: str = './dwca/output/'):
        self._extract_dwca(exclude_ext_files=exclude_ext_files)
        self._generate_eml()
        self._generate_meta()
        self._write_dwca(output_dwca_path)

    def delete_records(self, records_to_delete: CsvFileType, output_dwca_path: str = './dwca/output/'):
        self._extract_dwca()
        self._delete_records(records_to_delete)
        self._generate_eml()
        self._generate_meta()
        self._write_dwca(output_dwca_path)


    def create_dwca(self, core_csv: Union [CsvFileType, DataFrameType], ext_csv_list: list[CsvFileType],
                    output_dwca_path: str = './dwca/output/', validate_content: bool = True,
                    eml_content: str = ''):
        self._extract_csv_content(core_csv, CoreOrExtType.CORE)

        # Only validate core content
        if validate_content and not self._validate_content():
            raise SystemExit(Exception("Some validations error found. Dwca is not created."))

        # if multimedia files is supplied, do not attempt to convert associated media to multimedia
        if not any(ext.type == 'multimedia' for ext in ext_csv_list):
            image_ext = self._convert_associated_media_to_extension()
            if image_ext:
                ext_csv_list.append(image_ext)

        for ext in ext_csv_list:
            self._extract_csv_content(ext, CoreOrExtType.EXTENSION)

        self._generate_eml(eml_content)
        self._generate_meta()
        self._write_dwca(output_dwca_path)

    # Key lookup: For merging to update content and also used as lookup to link extensions to core records.
    # If not passed in, it obtains from collectory identifier. If passed in, this is used instead of from collectory identifiers
    #    - catered more for non occurrence type of dwca
    # regen_ids will generate new uuids for core csv and link coreids extensions to core records.
    # This will consume more time for large dwca
    # https://peps.python.org/pep-0484/#forward-references
    def merge_dwca(self, delta_dwca: BaseDwca, output_dwca_path: str, keys_lookup: dict = {}, extension_sync=False,
                   regen_ids: bool = False, validate_delta: bool = True):
        self._extract_dwca()
        delta_dwca._extract_dwca()
        self._set_keys(keys_lookup)
        delta_dwca._set_keys(keys_lookup)
        if validate_delta and not delta_dwca._validate_content():
            raise SystemExit(Exception("Some validations error found in the delta dwca. Dwca is not merged."))

        self._merge_contents(delta_dwca, extension_sync, regen_ids)
        self._generate_eml()
        self._generate_meta()
        self._write_dwca(output_dwca_path)

    def validate_dwca(self, content_keys: dict, error_file: str):
        self._extract_dwca()
        set_keys = self._set_keys(content_keys)
        content_type_to_validate = list(set_keys.keys())
        return self._validate_content(content_type_to_validate=content_type_to_validate, error_file=error_file)


    def validate_file(self, csv: CsvFileType, error_file: str):
        self._extract_csv_content(csv, CoreOrExtType.CORE)
        return self._validate_content(error_file=error_file)

    def sanitize_dwca(self, output_dwca_path: str):
        self._extract_dwca()
        self._merge_df_dwc_columns()
        self._generate_eml()
        self._generate_meta()
        self._write_dwca(output_dwca_path)
