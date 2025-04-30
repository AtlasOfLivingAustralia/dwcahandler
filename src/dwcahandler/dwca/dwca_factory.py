"""
Module contains factory class for Dwca. This is used to decide the type of darwin core class to perform the operation.

"""
import io
import logging
from typing import Union
import pandas as pd
from dwcahandler.dwca import ContentData, Dwca, Terms, Eml, MetaElementTypes, CSVEncoding, get_keys
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
log = logging.getLogger("DwcaFactoryManager")


class DwcaHandler:

    @staticmethod
    def list_terms() -> (pd.DataFrame, pd.DataFrame):
        return Terms().terms_df, Terms().class_df

    @staticmethod
    def list_class_rowtypes() :
        for name, member in MetaElementTypes.__members__.items():
            print(f"{name}: {member.value}")

    @staticmethod
    def get_contents_from_file_names(files: list, csv_encoding: CSVEncoding = CSVEncoding(),
                                     content_keys: dict[MetaElementTypes, list] = None, zf: ZipFile = None) \
            -> (ContentData, list[ContentData]):
        """Find the core content and extension contents from a list of file paths.
        Core content will always be event if present, otherwise, occurrence content

        :param files: list of files
        :param csv_encoding: delimiter for txt file. Default is comma delimiter txt files if not supplied
        :param content_keys: optional dictionary of MetaElementTypes and key list
                                      for eg. {MetaElementTypes.OCCURRENCE, ["occurrenceID"]}
        :param zf: Zipfile pointer if using
        :return dict of core content type and file name and dict containing ext content type and file name
        """
        def derive_type(file_list: list) -> dict[str, MetaElementTypes]:
            file_types = {}
            for file in file_list:
                if (filename := Path(file).stem.upper()) in dict(MetaElementTypes.__members__.items()).keys():
                    file_types[file] = dict(MetaElementTypes.__members__.items())[str(filename)]
            return file_types

        contents = derive_type(files)

        core_file = {k: v for k, v in contents.items() if v == MetaElementTypes.EVENT}
        if not core_file:
            core_file = {k: v for k, v in contents.items() if v == MetaElementTypes.OCCURRENCE}

        if core_file:
            core_filename = next(iter(core_file))
            core_type = core_file[core_filename]
            ext_files = {k: v for k, v in contents.items() if v != core_type}

            core_data = [core_filename] if not zf else io.TextIOWrapper(zf.open(core_filename), encoding="utf-8")
            core_content = ContentData(data=core_data,
                                       type=core_type, csv_encoding=csv_encoding,
                                       keys=get_keys(class_type=core_type,
                                                     override_content_keys=content_keys))
            ext_content = []
            for ext_file, ext_type in ext_files.items():
                ext_data = [ext_file] if not zf else io.TextIOWrapper(zf.open(ext_file), encoding="utf-8")
                ext_content.append(ContentData(data=ext_data,
                                               type=ext_type, csv_encoding=csv_encoding,
                                               keys=get_keys(class_type=ext_type,
                                                             override_content_keys=content_keys)))
            return core_content, ext_content
        else:
            raise ValueError("The core content cannot be determined. Please check filenames against the class type. "
                             "Use list_class_rowtypes to print the class types. ")

    """Perform various DwCA operations"""
    @staticmethod
    def create_dwca_from_file_list(files: list, output_dwca: Union[str, BytesIO],
                                   eml_content: Union[str, Eml] = '', csv_encoding: CSVEncoding = CSVEncoding(),
                                   content_keys: dict[MetaElementTypes, list] = None,
                                   extra_read_param: dict = None,
                                   validate_content: bool = True):
        """Helper function to create a dwca based on a list of txt files. The file names will determine the class type
           Builds event core dwca if event.txt is supplied,
           otherwise build an occurrence core dwca if occurrence.txt is supplied.

        :param files: List of txt files
        :param output_dwca: Where to place the resulting Dwca
        :param eml_content: eml content in string or Eml class
        :param csv_encoding: delimiter for txt file. Default is comma delimiter txt files if not supplied
        :param content_keys: optional dictionary of MetaElementTypes and key list
                                      for eg. {MetaElementTypes.OCCURRENCE, ["occurrenceID"]}
        :param extra_read_param: extra read param to use if any
        :param validate_content Validate the contents
        """
        core_content, ext_content_list = DwcaHandler.get_contents_from_file_names(files=files,
                                                                                  csv_encoding=csv_encoding,
                                                                                  content_keys=content_keys)
        DwcaHandler.create_dwca(core_csv=core_content, ext_csv_list=ext_content_list, output_dwca=output_dwca,
                                eml_content=eml_content, extra_read_param=extra_read_param,
                                validate_content=validate_content)

    @staticmethod
    def create_dwca_from_zip_content(zip_file: str, output_dwca: Union[str, BytesIO],
                                     eml_content: Union[str, Eml] = '', csv_encoding: CSVEncoding = CSVEncoding(),
                                     content_keys: dict[MetaElementTypes, list] = None,
                                     extra_read_param: dict = None,
                                     validate_content: bool = True):
        """Helper function to create a dwca based on a list of txt files in a zip file.
           The file names will determine the class type
           Builds event core dwca if event.txt is supplied,
           otherwise build an occurrence core dwca if occurrence.txt is supplied.

        :param zip_file: Zip file containing txt files
        :param output_dwca: Where to place the resulting Dwca
        :param eml_content: eml content in string or Eml class
        :param csv_encoding: delimiter for txt file. Default is comma delimiter txt files if not supplied
        :param content_keys: optional dictionary of class type and the key
                             for eg. {MetaElementTypes.OCCURRENCE, ["occurrenceID"]}
        :param extra_read_param: extra read param to use if any
        :param validate_content Validate the contents
        """
        with ZipFile(zip_file, 'r') as zf:
            files = zf.namelist()
            core_content, ext_content_list = DwcaHandler.get_contents_from_file_names(files=files,
                                                                                      csv_encoding=csv_encoding,
                                                                                      content_keys=content_keys,
                                                                                      zf=zf)
            DwcaHandler.create_dwca(core_csv=core_content, ext_csv_list=ext_content_list, output_dwca=output_dwca,
                                    eml_content=eml_content, extra_read_param=extra_read_param,
                                    validate_content=validate_content)
            zf.close()

    @staticmethod
    def create_dwca(core_csv: ContentData,
                    output_dwca: Union[str, BytesIO],
                    ext_csv_list: list[ContentData] = None,
                    validate_content: bool = True,
                    eml_content: Union[str, Eml] = '',
                    extra_read_param: dict = None):
        """Create a suitable DwCA from a list of CSV data

        :param core_csv: The core source
        :param ext_csv_list: A list of extension sources
        :param output_dwca: Where to place the resulting Dwca
        :param validate_content: Validate the DwCA before processing.
                                 Dwca is not created if the validation fails for the content.
                                 Current contents validated by default is Event and Occurrence
        :param eml_content: eml content in string or Eml class
        :param extra_read_param: extra read param to use if any
        """
        Dwca().create_dwca(core_csv=core_csv, ext_csv_list=ext_csv_list, output_dwca=output_dwca,
                           validate_content=validate_content, eml_content=eml_content,
                           extra_read_param=extra_read_param)

    @staticmethod
    def delete_records(dwca_file: Union[str, BytesIO], records_to_delete: ContentData,
                       output_dwca: Union[str, BytesIO]):
        """Delete core records listed in the records_to_delete file from DwCA.
        The specified keys listed in records_to_delete param must exist in the dwca core file

        :param dwca_file: The path to the DwCA or ByteIO of the DwCA
        :param records_to_delete: content containing the records to delete and the column key for mapping
        :param output_dwca: Where to place the resulting DwCA or the dwca output in memory
        """
        Dwca(dwca_file_loc=dwca_file).delete_records_in_dwca(records_to_delete=records_to_delete,
                                                             output_dwca=output_dwca)

    @staticmethod
    def merge_dwca(dwca_file: Union[str, BytesIO], delta_dwca_file: Union[str, BytesIO],
                   output_dwca: Union[str, BytesIO], keys_lookup: dict = None, extension_sync: bool = False,
                   validate_delta_content: bool = True):
        """Merge a DwCA with a delta DwCA of changes.

        :param dwca_file: The path to the existing DwCA
        :param delta_dwca_file: The path to the DwCA containing the delta
        :param output_dwca: Where to place the resulting
        :param keys_lookup: The keys defining a unique row
        :param extension_sync: Synchronise extensions
        :param validate_delta_content: Validate the delta DwCA before using
        """
        delta_dwca = Dwca(dwca_file_loc=delta_dwca_file)
        Dwca(dwca_file_loc=dwca_file).merge_dwca(delta_dwca=delta_dwca, output_dwca=output_dwca,
                                                 keys_lookup=keys_lookup, extension_sync=extension_sync,
                                                 validate_delta=validate_delta_content)

    @staticmethod
    def validate_dwca(dwca_file: Union[str, BytesIO], content_keys: dict = None,
                      extra_read_param: dict = None, error_df: pd.DataFrame = None):
        """Validate dwca for unique key and column for core content by default.
            If content_keys is supplied, the content is also validated.

        :param dwca_file: The path to the DwCA
        :param content_keys: a dictionary of class type and the key.
                When content_keys are provided, validation will be performed on the content as well.
                             for eg. {MetaElementTypes.OCCURRENCE, "occurrenceID"}
        :param extra_read_param: extra read param to use if any
        :param error_df: The reference to the dataframe to write errors to. If None, errors are logged
        """
        return Dwca(dwca_file_loc=dwca_file).validate_dwca(content_keys=content_keys, error_df=error_df,
                                                           extra_read_param=extra_read_param)

    @staticmethod
    def validate_file(csv_file: ContentData, extra_read_param: dict = None, error_df: str = None):
        """Test a CSV file for consistency

        :param csv_file: The path to the CSV
        :param extra_read_param: extra read param to use if any
        :param error_df: The reference to the dataframe error to write errors to, if None log errors
        """
        return Dwca().validate_file(csv=csv_file, extra_read_param=extra_read_param, error_df=error_df)
