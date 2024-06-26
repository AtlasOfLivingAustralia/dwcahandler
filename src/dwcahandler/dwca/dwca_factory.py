"""
Module contains factory class for Dwca. This is used to decide the type of darwin core class to perform the operation.

"""

import logging
from typing import Union
import pandas as pd
from dwcahandler.dwca import CsvFileType, DataFrameType, Dwca, Terms, Eml


logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
log = logging.getLogger("DwcaFactoryManager")


class DwcaHandler:

    @staticmethod
    def list_dwc_terms() -> pd.DataFrame:
        return Terms().dwc_terms_df

    """Perform various DwCA operations"""

    @staticmethod
    def create_dwca(core_csv: Union[CsvFileType, DataFrameType],
                    output_dwca_path: str,
                    ext_csv_list: list[Union[CsvFileType, DataFrameType]] = None,
                    validate_content: bool = True,
                    eml_content: Union[str, Eml] = ''):
        """Create a suitable DwCA from a list of CSV files

        :param core_csv: The core source
        :param ext_csv_list: A list of extension sources
        :param output_dwca_path: Where to place the resulting Dwca
        :param validate_content: Validate the DwCA before processing
        :param eml_content: eml content in string or Eml class
        """
        Dwca().create_dwca(core_csv=core_csv, ext_csv_list=ext_csv_list, output_dwca_path=output_dwca_path,
                           validate_content=validate_content, eml_content=eml_content)

    @staticmethod
    def remove_extension_files(dwca_file: str, ext_files: list, output_dwca_path: str):
        """Load a DwCA and remove extension files from it

        :param dwca_file: The path to the DwCA
        :param ext_files: A list of extension files to delete
        :param output_dwca_path: Where to place the resulting DwCA
        """
        Dwca(dwca_file_loc=dwca_file).remove_extensions(exclude_ext_files=ext_files,
                                                        output_dwca_path=output_dwca_path)

    @staticmethod
    def delete_records(dwca_file: str, records_to_delete: CsvFileType,
                       output_dwca_path: str):
        """Delete core records listed in the records_to_delete file from DwCA.
        The specified keys listed in records_to_delete param must exist in the dwca core file

        :param dwca_file: The path to the DwCA
        :param records_to_delete: File containing the records to delete and the column key for mapping
        :param output_dwca_path: Where to place the resulting DwCA
        """
        Dwca(dwca_file_loc=dwca_file).delete_records_in_dwca(records_to_delete=records_to_delete,
                                                             output_dwca_path=output_dwca_path)

    @staticmethod
    def merge_dwca(dwca_file: str, delta_dwca_file: str, output_dwca_path: str, keys_lookup: dict = None,
                   extension_sync: bool = False, regen_ids: bool = False, validate_delta_content: bool = True):
        """Merge a DwCA with a delta DwCA of changes.

        :param dwca_file: The path to the existing DwCA
        :param delta_dwca_file: The path to the DwCA containing the delta
        :param output_dwca_path: Where to place the resulting
        :param keys_lookup: The keys defining a unique row
        :param extension_sync: Synchronise extensions
        :param regen_ids: Regenerate the unique ids used to tye core and extension records together
        :param validate_delta_content: Validate the delta DwCA before using
        """
        delta_dwca = Dwca(dwca_file_loc=delta_dwca_file)
        Dwca(dwca_file_loc=dwca_file).merge_dwca(delta_dwca=delta_dwca, output_dwca_path=output_dwca_path,
                                                 keys_lookup=keys_lookup, extension_sync=extension_sync,
                                                 regen_ids=regen_ids, validate_delta=validate_delta_content)

    @staticmethod
    def validate_dwca(dwca_file: str, keys_lookup: dict = None, error_file: str = None):
        """Test a dwca for consistency

        :param dwca_file: The path to the DwCA
        :param keys_lookup: The keys that identify a unique record
        :param error_file: The file to write errors to. If None, errors are logged
        """
        return Dwca(dwca_file_loc=dwca_file).validate_dwca(keys_lookup, error_file)

    @staticmethod
    def validate_file(csv_file: CsvFileType, error_file: str = None):
        """Test a CSV file for consistency

        :param csv_file: The path to the CSV
        :param error_file: The file to write errors to, if None log errors
        """
        return Dwca().validate_file(csv_file, error_file)
