"""
Module contains factory class for Dwca. This is used to decide the type of darwin core class to perform the operation.

"""

from abc import ABCMeta, abstractmethod
import logging
from pathlib import Path
from typing import Union
import pandas as pd
from dwcahandler.dwca import CsvFileType, DataFrameType, BaseDwca, Dwca, LargeDwca, Terms, Eml


logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
log = logging.getLogger("DwcaFactoryManager")


class BaseDwcaFactory(metaclass=ABCMeta):
    """Abstract DwCA instance creator"""

    @abstractmethod
    def get_dwca(self, **kwargs) -> BaseDwca:
        """Get a Dwca

        :return: The corresponding Dwca
        """
        pass

    @abstractmethod
    def get_dwca_from_dwca_file(self, dwca, **kwargs) -> BaseDwca:
        """Get a DwCA from a file

        :param dwca: The path to the DwCA
        :return:
        """
        pass


class DwcaFactory(BaseDwcaFactory):
    """Default Dwca factory which returns ALA-style collectory linked Dwca instances"""

    def get_dwca(self, **kwargs) -> BaseDwca:
        """Get a Dwca for a data resource

        :return: The corresponding Dwca
        """
        return Dwca()

    def get_dwca_from_dwca_file(self, dwca, **kwargs) -> BaseDwca:
        """Get a DwCA from a file

        :param dwca: The path to the DwCA
        :return: The corresponding Dwca
        """
        return Dwca(dwca_file_loc=dwca)


class LargeDwcaFactory(BaseDwcaFactory):
    """Large Dwca factory which returns ALA-style collectory linked large dwca instances backed by a work folder"""

    def _extract_param(self, kwargs):
        work_dir = './dwca/output/pickle'
        chunk_size = LargeDwca.CHUNK_SIZE
        if 'work_dir' in kwargs:
            work_dir = kwargs['work_dir']
        if 'chunk_size' in kwargs :
            chunk_size = kwargs['chunk_size']
        return (work_dir, chunk_size)

    def get_dwca(self, **kwargs) -> BaseDwca: # work_dir, chunk_size) -> BaseDwca:
        """Get a large DwCA

        :param work_dir: The work directory
        :param chunk_size: The chunk size
        :return: The corresponding LargeDwca
        """
        work_dir, chunk_size = self._extract_param(kwargs)
        return LargeDwca(temp_folder=work_dir, CHUNK_SIZE=chunk_size)

    def get_dwca_from_dwca_file(self, dwca, **kwargs) -> BaseDwca:
        """Get a large DwCA from a file

        :param dwca: The path to the file
        :param work_dir: The work directory
        :param chunk_size: The chunk size
        :return: The corresponding LargeDwca
        """
        work_dir, chunk_size = self._extract_param(kwargs)
        return LargeDwca(dwca_file_loc=dwca, temp_folder=work_dir, CHUNK_SIZE=chunk_size)


class DwcaFactoryManager:
    """A class that chooses the appropriate Dwca implementation based on file size"""
    CSV_FILE_SIZE_THRESHOLD: float = 1.0  # 1GB
    DWCA_FILE_SIZE_THRESHOLD: float = 0.5

    def __get_file_size(files: list):
        """Determine the total file size of a collection of files.

        :return: The total file size in gigabytes
        """
        total_file_size = 0
        for file in files:
            total_file_size += Path(file).stat().st_size
        return total_file_size / (1024 * 1024 * 1024)

    @staticmethod
    def get_dwca_from_csv(csv_file: Union [list, pd.DataFrame], use_chunking: bool = False, work_dir: str = './dwca/output/pickle',
                          chunk_size=50000, calculate_size: bool = True) -> BaseDwca:
        """Get a DwCA from a list of CSV files

        :param csv_file: The location of the CSV files for the DwCA
        :param use_chunking: Use chunking by default
        :param work_dir: The work directory for large files
        :param chunk_size: The chunk size
        :param calculate_size: Calculate the size of the file and determine whether to
               use a large or simple Dwca instance
        :return: A suitable DwCA
        """
        # check csv_file size, then return the appropriate dwca factory
        # 1GB is threshold currently
        if (use_chunking or
           (isinstance(csv_file, list) and DwcaFactoryManager.__get_file_size(
                    csv_file) > DwcaFactoryManager.CSV_FILE_SIZE_THRESHOLD and calculate_size)):
            return LargeDwcaFactory().get_dwca(work_dir=work_dir, chunk_size=chunk_size)
        else:
            return DwcaFactory().get_dwca()

    @staticmethod
    def get_dwca_from_dwca_file(dwca_file: str, use_chunking: bool = False, work_dir='./dwca/output/pickle',
                                chunk_size=50000, calculate_size: bool = False) -> BaseDwca:
        """Get a DwCA from a dwca file

            :param dwca_file: The path to the DwCA
            :param use_chunking: Use chunking by default
            :param work_dir: The work directory for large files
            :param chunk_size: The chunk size
            :param calculate_size: Calculate the size of the file and determine whether to
                   use a large or simple Dwca instance
            :return: A suitable DwCA
            """
        if (use_chunking or
                (DwcaFactoryManager.__get_file_size(
                    [dwca_file]) > DwcaFactoryManager.DWCA_FILE_SIZE_THRESHOLD and calculate_size)):
            return LargeDwcaFactory().get_dwca_from_dwca_file(dwca=dwca_file, work_dir=work_dir,
                                                              chunk_size=chunk_size)
        else:
            return DwcaFactory().get_dwca_from_dwca_file(dwca=dwca_file)


class DwcaHandler:

    @staticmethod
    def list_dwc_terms() -> pd.DataFrame:
        return Terms().dwc_terms_df


    """Perform various DwCA operations"""

    @staticmethod
    def create_dwca(core_csv: Union[CsvFileType, DataFrameType], ext_csv_list: list[Union [CsvFileType, DataFrameType]] = [],
                    output_dwca_path: str = './dwca/output/', work_dir: str = './dwca/output/pickle',
                    use_chunking: bool = False, chunk_size=1000, calculate_size=True, validate_content: bool = True,
                    eml_content: Union [str, Eml] = ''):
        """Create a suitable DwCA from a list of CSV files

        :param core_csv: The core source
        :param ext_csv_list: A list of extension sources
        :param output_dwca_path: Where to place the resulting Dwca
        :param work_dir: The work directory
        :param use_chunking: Use chunking for large files
        :param chunk_size: The
        :param calculate_size: Check the file size and use it to decide whether to chunk or not
        :param validate_content: Valudate the DwCA before processing
        :param eml_content: eml content in string or Eml class
        """
        core_csv_source = core_csv.files if isinstance(core_csv, CsvFileType) else core_csv.df
        dwca = DwcaFactoryManager.get_dwca_from_csv(csv_file=core_csv_source, use_chunking=use_chunking,
                                                    work_dir=work_dir,
                                                    chunk_size=chunk_size, calculate_size=calculate_size)
        dwca.create_dwca(core_csv=core_csv, ext_csv_list=ext_csv_list, output_dwca_path=output_dwca_path,
                         validate_content=validate_content, eml_content=eml_content)

    @staticmethod
    def remove_extension_files(dwca_file: str, ext_files: list, output_dwca_path: str = './dwca/output/'):
        """Load a DwCA and remove extension files from it

        :param dwca_file: The path to the DwCA
        :param ext_files: A list of extension files to delete
        :param output_dwca_path: Where to place the resulting DwCA
        """
        dwca = DwcaFactoryManager.get_dwca_from_dwca_file(dwca_file=dwca_file)
        dwca.remove_extensions(exclude_ext_files=ext_files, output_dwca_path=output_dwca_path)

    @staticmethod
    def delete_records(dwca_file: str, records_to_delete: CsvFileType,
                       output_dwca_path: str = './dwca/output/',
                       work_dir: str = './dwca/output/pickle', use_chunking: bool = False, chunk_size=1000,
                       calculate_size=False):
        dwca = DwcaFactoryManager.get_dwca_from_dwca_file(dwca_file=dwca_file, use_chunking=use_chunking,
                                                          work_dir=work_dir, chunk_size=chunk_size,
                                                          calculate_size=calculate_size)
        dwca.delete_records_in_dwca(records_to_delete=records_to_delete, output_dwca_path=output_dwca_path)

    @staticmethod
    def merge_dwca(dwca_file: str, delta_dwca_file: str, output_dwca_path: str, keys_lookup: dict = {},
                   extension_sync: bool = False,
                   regen_ids: bool = False, work_dir: str = './dwca/output/pickle', use_chunking: bool = False,
                   chunk_size: int = 100000,
                   calculate_size: bool = True, validate_delta_content: bool = True):
        """Merge a DwCA with a delta DwCA of changes.

        :param dwca_file: The path to the existing DwCA
        :param delta_dwca_file: The path to the DwCA containing the delta
        :param output_dwca_path: Where to place the resulting
        :param keys_lookup: The keys defining a unique row (be default retruved from the data resource)
        :param extension_sync: Synchronise extensions
        :param regen_ids: Regenerate the unique ids used to tye core and extension records together
        :param work_dir: The path to the work directory
        :param use_chunking: Use chunking for large files
        :param chunk_size: The chunk size
        :param calculate_size: Determine wether to use chunked or in-memory processing automatically
        :param validate_delta_content: Validate the delta DwCA before using
        """
        dwca = DwcaFactoryManager.get_dwca_from_dwca_file(dwca_file=dwca_file, use_chunking=use_chunking,
                                                          work_dir=work_dir,
                                                          chunk_size=chunk_size, calculate_size=calculate_size)
        if (isinstance(dwca, LargeDwca)):
            use_chunking = True

        delta_dwca = DwcaFactoryManager.get_dwca_from_dwca_file(dwca_file=delta_dwca_file,
                                                                use_chunking=use_chunking, work_dir=work_dir,
                                                                chunk_size=chunk_size, calculate_size=calculate_size)
        dwca.merge_dwca(delta_dwca=delta_dwca, output_dwca_path=output_dwca_path, keys_lookup=keys_lookup,
                        extension_sync=extension_sync,
                        regen_ids=regen_ids, validate_delta=validate_delta_content)

    @staticmethod
    def sanitize_dwca(dwca_file: str, output_dwca_path: str, work_dir: str = './dwca/output/pickle',
                      use_chunking: bool = False,
                      chunk_size: int = 100000, calculate_size: bool = True):
        """Read a dwca and clean up any issues or mess

        :param dwca_file: The path to the DwCA
        :param output_dwca_path: The path to write the sanitized DwCA to
        :param work_dir: The work directory for chunked data
        :param use_chunking: Use chunking
        :param chunk_size: The chunk size
        :param calculate_size: Decide whether to use chunking based on on input file size
         """
        dwca = DwcaFactoryManager.get_dwca_from_dwca_file(dwca_file=dwca_file, use_chunking=use_chunking,
                                                          work_dir=work_dir,
                                                          chunk_size=chunk_size, calculate_size=calculate_size)
        dwca.sanitize_dwca(output_dwca_path=output_dwca_path)

    @staticmethod
    def validate_dwca(dwca_file: str, keys_lookup: dict = {}, work_dir: str = './dwca/output/pickle',
                      use_chunking: bool = False,
                      chunk_size: int = 100000, calculate_size: bool = True, error_file: str = None):
        """Test a dwca for consistency

        :param dwca_file: The path to the DwCA
        :param keys_lookup: The keys that identify a unique record
        :param work_dir: The work directory for chunked data
        :param use_chunking: Use chunking
        :param chunk_size: The chunk size
        :param calculate_size: Decide whether to use chunking based on on input file size
        :param error_file: The file to write errors to. If None, errors are logged
        """
        dwca = DwcaFactoryManager.get_dwca_from_dwca_file(dwca_file=dwca_file, use_chunking=use_chunking,
                                                          work_dir=work_dir,
                                                          chunk_size=chunk_size, calculate_size=calculate_size)
        return dwca.validate_dwca(keys_lookup, error_file)

    @staticmethod
    def validate_file(csv_file: CsvFileType, work_dir: str = './dwca/output/pickle',
                      use_chunking: bool = False,
                      chunk_size: int = 100000, calculate_size: bool = True, error_file: str = None):
        """Test a CSV file for consistency

        :param csv_file: The path to the CSV
        :param work_dir: The work directory for chunked data
        :param use_chunking: Use chunking
        :param chunk_size: The chunk size
        :param calculate_size: Decide whether to use chunking based on on input file size
        :param error_file: The file to write errors to, if None log errors
        """
        dwca = DwcaFactoryManager.get_dwca_from_csv(csv_file=csv_file.files, use_chunking=use_chunking,
                                                    work_dir=work_dir,
                                                    chunk_size=chunk_size, calculate_size=calculate_size)
        return dwca.validate_file(csv_file, error_file)
