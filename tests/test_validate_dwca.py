from io import BytesIO
from zipfile import ZipFile
import zipfile
from pathlib import Path
from dwcahandler import DwcaHandler
import logging
import pytest

input_folder = "./input_files/dwca"


def make_zip_from_folder_contents(folder: str):
    zip_buffer = BytesIO()
    with ZipFile(file=zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
        for path in Path(folder).rglob("*"):
            zf.write(path, arcname=path.name)
        zf.close()
    return zip_buffer


class TestValidateDwca:

    def test_validate_dwca(self):
        """
        Test for read and extract dwca. Validate core content
        """
        simple_dwca = make_zip_from_folder_contents(f"{input_folder}/dwca-sample1")
        keys_lookup = {'occurrence': 'occurrenceID'}
        dwca_result = DwcaHandler.validate_dwca(dwca_file=simple_dwca, keys_lookup=keys_lookup)
        assert dwca_result

    def test_validate_dwca2(self):
        """
        Test for read and extract dwca. Validate core content
        """
        simple_dwca = make_zip_from_folder_contents(f"{input_folder}/dwca-sample2")
        keys_lookup = {'occurrence': 'occurrenceID'}
        dwca_result = DwcaHandler.validate_dwca(dwca_file=simple_dwca, keys_lookup=keys_lookup)
        assert dwca_result

    def test_empty_keys(self, caplog):
        """
        Test for read and extract dwca. Validate core content with empty keys
        """
        caplog.set_level(logging.INFO)
        simple_dwca = make_zip_from_folder_contents(f"{input_folder}/dwca-sample3")
        keys_lookup = {'occurrence': 'occurrenceID'}
        dwca_result = DwcaHandler.validate_dwca(dwca_file=simple_dwca, keys_lookup=keys_lookup)
        assert not dwca_result
        assert "Empty values found in ['occurrenceID']. Total rows affected: 1" in caplog.messages
        assert "Empty values found in dataframe row: [0]" in caplog.messages

    def test_duplicate_key(self, caplog):
        """
        Test for read and extract dwca. Validate core content with duplicate keys
        """
        caplog.set_level(logging.INFO)
        simple_dwca = make_zip_from_folder_contents(f"{input_folder}/dwca-sample4")
        keys_lookup = {'occurrence': 'catalogNumber'}
        dwca_result = DwcaHandler.validate_dwca(dwca_file=simple_dwca, keys_lookup=keys_lookup)
        assert not dwca_result
        assert "Duplicate ['catalogNumber'] found. Total rows affected: 3" in caplog.messages
        assert "Duplicate values: ['014800' '014823']" in caplog.messages

    def test_duplicate_columns_in_dwca(self):
        """
        Test for read and extract dwca. Validate duplicate columns specified in metadata of dwca
        """
        simple_dwca = make_zip_from_folder_contents(f"{input_folder}/dwca-sample5")
        keys_lookup = {'occurrence': 'catalogNumber'}

        with pytest.raises(ValueError) as exc_info:
            DwcaHandler.validate_dwca(dwca_file=simple_dwca, keys_lookup=keys_lookup)

        assert "Duplicate columns ['catalogNumber'] specified in the " \
                   "metadata for occurrence.csv" in str(exc_info.value)
