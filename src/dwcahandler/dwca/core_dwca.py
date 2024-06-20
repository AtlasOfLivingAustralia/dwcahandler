"""
Module contains the Darwin Core operations

"""

from __future__ import annotations
import csv
import logging
import re
import uuid
import zipfile
from dataclasses import dataclass, field, MISSING, asdict
from pathlib import Path
from typing import Union
import io
from zipfile import ZipFile
import mimetypes
import pandas as pd
from pandas.errors import EmptyDataError
from pandas.io import parsers
from numpy import nan
from dwcahandler.dwca import (BaseDwca, CsvFileType, DataFrameType, CSVEncoding, CoreOrExtType,
                              MetaElementTypes, MetaElementInfo, MetaDwCA, Eml, Stat,
                              record_diff_stat, Defaults)

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)
log = logging.getLogger("Dwca")


@dataclass
class DfContent:
    """A data frame with associated schema and metadata"""
    meta_info: MetaElementInfo
    df_content: pd.DataFrame = field(default_factory=pd.DataFrame)
    keys: list[str] = field(init=False, default_factory=list)
    stat: Stat = Stat(0)


@dataclass
class Dwca(BaseDwca):
    """
    A concrete implementation of a Darwin Core Archive.
    """
    dwca_file_loc: str = field(default='./')
    core_content: DfContent = field(init=False)
    ext_content: list[DfContent] = field(init=False, default_factory=list)
    defaults_prop: Defaults = field(init=False, default_factory=Defaults)
    meta_content: MetaDwCA = field(init=False)
    eml_content: str = field(init=False, default=None)
    embedded_files: list[Path] = field(init=False, default_factory=list)

    def __post_init__(self):
        self.meta_content = MetaDwCA(eml_xml_filename=self.defaults_prop.eml_xml_filename)

    def generate_eml(self, eml_content: Union[str, Eml] = ""):
        """
        Create the EML for the data source.

        :param: eml_content: eml string
        """
        eml_content_str = eml_content.build_eml_xml() if (
            isinstance(eml_content, Eml)) else eml_content

        if eml_content_str:
            self.eml_content = eml_content_str

    def generate_meta(self):
        """Create a meta-file description for the DwCA.

        The resulting meta-file is stored in the meta_content attribute.
        """
        self.meta_content.create()

    def count_stat(self, content):
        """ Get the length of a content frame

        :param content: The content frame
        :return: The number of records in the frame
        """
        return len(content)

    def _update_core_ids(self, core_df):
        """Generate core identifiers for a core data frame.

        UUID identifiers are generated for each row in the core data frame.
        These identifiers can be used to link core and extension records when no immediately
        useful identifier is available in the source data.

        :param core_df: The data frame to generate identifiers for
        """
        if 'id' not in core_df.columns.to_list():
            core_df.insert(0, 'id', core_df.apply(lambda _: uuid.uuid4(), axis=1), False)
        else:
            core_df['id'] = core_df['id'].map(lambda _: uuid.uuid4())

    def _update_df(self, to_update_df, lookup_df, update_field, from_update_field):
        """Update a data frame via lookup

        :param to_update_df: The data frame to update
        :param lookup_df: The data frame that contains the updates
        :param update_field: The field top update
        :param from_update_field: The field to get the updated data from
        :return: The number of records updated
        """
        exist = to_update_df.index.isin(lookup_df.index)
        # Note: update by querying single level index is not working??!!
        # exist = to_update_df.index.get_level_values(index_lookup_col).isin(lookup_df.index)
        if len(to_update_df.loc[exist]) > 0:
            to_update_df.loc[exist, update_field] = lookup_df[from_update_field]

        return len(to_update_df.loc[exist])

    def _update_extension_ids(self, csv_content, core_df_content, link_col: list):
        """Update the extension tables with (usually generated) identifiers
            from a core data frame.

        DwCAs only allow a single link identifier.
        If the link between the core and extension requires multiple fields, then an identifier
        column needs to be generated and linked across both data frames.

        :param csv_content: The extension to update
        :param core_df_content: The core data frame
        :param link_col: The columns that link the extension to the core
        """
        if 'coreid' in csv_content:
            csv_content.pop('coreid')

        # Having link_col as index and column raises ambiguous error in merge
        if (set(link_col).issubset(set(csv_content.columns.to_list())) and
                set(link_col).issubset(set(csv_content.index.names))):
            csv_content.reset_index(inplace=True, drop=True)

        csv_content = csv_content.merge(core_df_content.loc[:, 'id'],
                                        left_on=link_col,
                                        right_on=link_col, how='inner')

        if 'id' in csv_content.columns.to_list():
            col = csv_content.pop('id')
            csv_content.insert(0, col.name, col)
            csv_content.rename(columns={"id": "coreid"}, inplace=True)

        return csv_content

    def _update_associated_files(self, assoc_files: list[str]):
        """Update the internal list of additional files.

        :param assoc_files: The list of associated fields.
         """
        self.embedded_files = [Path(file_path) for file_path in assoc_files]

    def _read_header(self, df_content: pd.DataFrame) -> list[str]:
        """Get the column names of a data frame.

        :param df_content: The data frame content.
        :return: The data frame columns as a list
        """
        headers = df_content.columns.to_list()
        return headers

    def _set_content(self, csv_content, meta_element_type):
        """Create a content description for a data frame.

        :param csv_content: The data frame
        :param meta_element_type: The CSV file description (encoding, name, type etc)
        :return: A content object encapsulating the content
        """
        return DfContent(df_content=csv_content, meta_info=meta_element_type,
                         stat=Stat(self.count_stat(csv_content)))

    def extract_dwca(self, exclude_ext_files: list = None):
        """Read a DwCA file into this object.
        The archive is expected to be in zip file form, located at the `dwca_file_loc` attribute.
        The content and meta-information are initialised from the archive.

        :param exclude_ext_files: Ignore the following file names
        """
        def convert_values(v):
            invalid_values = self.defaults_prop.translate_table.keys()
            return self.defaults_prop.translate_table[v] if v in invalid_values else v

        with ZipFile(self.dwca_file_loc, 'r') as zf:

            files = zf.namelist()

            log.info("Reading from %s", self.dwca_file_loc)
            with zf.open(self.defaults_prop.meta_xml_filename) as meta_xml_file:
                self.meta_content.read_meta_file(meta_xml_file)
                meta_xml_file.close()

            if self.meta_content.eml_xml_filename in files:
                with io.TextIOWrapper(zf.open(self.meta_content.eml_xml_filename),
                                      encoding="utf-8") as eml_xml_file:
                    # read as string
                    self.eml_content = eml_xml_file.read()
                    eml_xml_file.close()

            if exclude_ext_files and len(exclude_ext_files) > 0:
                self.meta_content.remove_meta_elements(exclude_ext_files)

            for meta_elm in self.meta_content.meta_elements:
                csv_file_name = meta_elm.meta_element_type.file_name
                with io.TextIOWrapper(zf.open(csv_file_name), encoding="utf-8") as csv_file:
                    dwc_headers = [f.field_name for f in meta_elm.fields if f.index is not None]
                    csv_encoding = {key: convert_values(value) for key, value in
                                    asdict(meta_elm.meta_element_type.csv_encoding).items()}
                    csv_content = self._read_csv(
                        csv_file, columns=dwc_headers,
                        csv_encoding_param=CSVEncoding(**csv_encoding),
                        ignore_header_lines=int(meta_elm.meta_element_type.ignore_header_lines))
                    if meta_elm.meta_element_type.core_or_ext_type == CoreOrExtType.CORE:
                        self.core_content = self._set_content(csv_content,
                                                              meta_elm.meta_element_type)
                    else:
                        self.ext_content.append(self._set_content(csv_content,
                                                                  meta_elm.meta_element_type))
                    csv_file.close()

            zf.close()

    def _add_new_columns(self, df_content, delta_df_content):
        """Add additional columns to a data frame

        New columns are initialised to vectors of NaN

        :param df_content: The base data frame content
        :param delta_df_content: The data frane content to add
        """
        df_columns = df_content.columns.to_list()
        delta_df_columns = delta_df_content.columns.to_list()
        new_columns = list(set(delta_df_columns) - set(df_columns))
        if len(new_columns) > 0:
            df_content[new_columns] = nan

        return new_columns

    def _update_values(self, df_content, delta_df_content, keys, stat):
        """Update data frame content with a delta.

        Identifier and key columns are not updated.

        :param df_content: The content data frame to update
        :param delta_df_content: The updating data frame
        :param keys: The key columns
        :param stat: The statistics object to update
        :return: The updated content
        """
        # Extract columns that need updating, excluding self.keys and id
        non_update_column = ['id', 'coreid']
        non_update_column.extend(keys)
        update_columns = [i for i in delta_df_content.columns.to_list()
                          if i not in non_update_column]

        updated_rows = self._update_df(df_content, delta_df_content,
                                       update_columns, update_columns)
        stat.add_update_stat(updated_rows)

        return df_content

    def _find_records_exist_in_both(self, core_content, delta_core_content):
        """Find the records that are to be updated

        :param core_content: The content to update
        :param delta_core_content: The content that contains the update
        :return: An index of the core content that is also present in the delta
        """
        return core_content[core_content.index.isin(delta_core_content.index)].index

    def _delete_old_ext_records(self, content, core_content, delta_core_content, core_keys):
        """Drop all extension rows where core records are exist in both content and delta

        :param content: The extension
        :param core_content: The core records
        :param delta_core_content: The delta update
        :param core_keys: The key fields
        """
        core_exist = self._find_records_exist_in_both(core_content, delta_core_content)
        exist = content.df_content.index
        for key in core_keys:
            exist = exist.get_level_values(key).isin(core_exist)
        if len(content.df_content.loc[exist]) > 0:
            log.info("Number of rows dropped from extension %s because of ext_sync: %s",
                     content.meta_info.type.name, str(len(content.df_content.loc[exist])))
            content.df_content.drop(content.df_content.iloc[exist].index, inplace=True)

    def _add_new_rows(self, df_content, new_rows):
        """
        Append new rows to a content frame.

        :param df_content: The content frame
        :param new_rows: The new rows to add
        :return: A data frame with the rows concatenated
        """
        return pd.concat([df_content, new_rows], ignore_index=False)

    def set_keys(self, keys: dict = None):
        """Set unique identifier keys.

        :param keys: The dict of keys for content
        :return: The keys which have been set for the content
        """
        set_keys = {}
        if keys and len(keys) > 0:
            for k, v in keys.items():
                dwca_content, _ = self._get_content(MetaElementTypes.get_element(k).row_type_ns)
                # If found then set the key for the content
                if dwca_content:
                    dwca_content.keys = [v] if isinstance(v, str) else v
                    set_keys[k] = v

        return set_keys

    def _update_meta_fields(self, content):
        """Update meta content fields by reading the content frame"""
        fields = self._read_header(content.df_content)
        self.meta_content.update_meta_element(meta_element_info=content.meta_info, fields=fields)

    def _filter_content(self, df_content, delta_df_content):
        """Filter delta content that is not already in the existing content

        :param df_content: The existing content
        :param delta_df_content: The delta
        :return: A content frane containing only those records in delta_df_content not in df_content
        """
        return delta_df_content[~delta_df_content.index.isin(df_content.index)]

    def _add_col_and_update_values(self, df_content, delta_df_content, keys, update, stat):
        """Add new columns, if needed, and update any values

        :param df_content: The existing content
        :param delta_df_content: The delta to apply
        :param keys: The unique term keys for the content
        :param update: If true, update the values from the delta
        :param stat: The statistics to update
        :return: A list of any new columns
        """
        new_columns = self._add_new_columns(df_content, delta_df_content)

        if update:
            self._update_values(df_content, delta_df_content, keys, stat)

        return new_columns

    @record_diff_stat
    def _merge_df_content(self, content, delta_content, keys, update=True):
        """Merge a delta into an existing content frame and update the meta-file description

        :param content: The existing content frame
        :param delta_content: The delta to apply
        :param keys: The list of columns that uniquely identify a core record
        :param update: If true update existing records (otherwise just add new records)
        :return: A new content frame with changes to existing values made and
                additional records appended
        """
        new_columns = self._add_col_and_update_values(content.df_content, delta_content.df_content,
                                                      keys, update, content.stat)
        if len(new_columns) > 0:
            log.info("New columns added: %s", ','.join(new_columns))
            self._update_meta_fields(content)

        new_rows = self._filter_content(content.df_content, delta_content.df_content)

        # return the merged content
        return self._add_new_rows(content.df_content, new_rows)

    def _find_duplicate_columns(self, content):
        """Find any duplicated columns in a content frame

        :param content: The content frame
        :return: A list of duplicated fields
        """
        all_columns = self._read_header(content.df_content)
        sanitized_fields = self.meta_content.map_headers(all_columns)
        list_fields = [f.field_name for f in sanitized_fields]
        dup_fields = [item for item in set(list_fields) if list_fields.count(item) > 1]
        if len(dup_fields) > 0:
            log.error("Duplicate fields found: %s", ','.join(dup_fields))
        return dup_fields

    def merge_df_dwc_columns(self):
        """Merge any duplicated columns in the core content.

        Note that only yhr core content is merged.
        """
        content = self.core_content
        dup_fields = self._find_duplicate_columns(content)
        updated = False

        if len(dup_fields) > 0:
            all_columns = self._read_header(content.df_content)
            for dup in dup_fields:
                columns = list(filter(
                    lambda term, dp_term=dup: re.fullmatch(pattern=f".*?{dp_term}", string=term),
                    all_columns))
                if dup in columns:
                    other_col = columns[1]
                    df = self.core_content.df_content
                    self.core_content.stat.set_update_stat(0)
                    self._update_column(df, dup, other_col, self.core_content.stat)
                    log.info("columns %s updated with values from %s. Here is the stat: %s",
                             dup, other_col, str(self.core_content.stat))
                    log.info("column %s dropped", other_col)
                    updated = True

        if updated:
            self._update_meta_fields(content)

    def _update_column(self, df, col, other_col, stat):
        """Update a data frame, copying values from another column with non-null entries.

        Updates come from two sources:

        - A direct value from other_col
        - An entry in `dynamicProperties` keyed to `other_col`

        :param df: The data frame to update
        :param col: The column to update
        :param other_col: The column to copy values from
        :param stat: A statistics object to record updates
        :return:
        """
        # Step 1: if dcterms_xxx is not null, replace the dcterms_xxx into xxx field,
        # also clean up the dynamic properties for the rows
        to_update = df[other_col].notnull()  # df[dup].isnull() &
        df.loc[to_update, col] = df.loc[to_update, other_col]
        stat.add_update_stat(len(df[to_update]))
        log.info(df.loc[to_update, col])
        # Also cleanup the dynamicProperties
        df.loc[to_update, 'dynamicProperties'] = df.loc[to_update, 'dynamicProperties'].str.replace(
            f'(,?)("dcterms_{col}":".*?")(?=,?)', '', regex=True).str.replace('{,', '{', regex=True)

        # Step 2: Check if col value is still null. If null, extract from the dynamic properties
        to_update = df[col].isnull()
        df.loc[to_update, col] = df.loc[to_update, 'dynamicProperties'].\
            str.extract(rf'.*?"dcterms_{col}":"(.*?)"')[0]
        df.loc[to_update, 'dynamicProperties'] = (
                    df.loc[to_update, 'dynamicProperties'].str.
                    replace(f'(,?)("dcterms_{col}":".*?")(?=,?)', '', regex=True).str.
                    replace('{,', '{', regex=True))
        stat.add_update_stat(len(df[to_update]))
        df.drop(columns=[other_col], inplace=True)

    def _regenerate_coreids(self):
        """Rebuild core identifiers.

        Adds an `id` column if one does not exist and generates a UUID (4) for each core row.
        Corresponding extension rows are updated to match.
        """
        self.core_content.df_content['id'] = (self.core_content.df_content['id'].
                                              map(lambda _: uuid.uuid4()))
        for content in self.ext_content:
            content.df_content = self._update_extension_ids(
                content.df_content, self.core_content.df_content, self.core_content.keys)

    def _build_index_for_content(self, df_content: pd.DataFrame, keys: list):
        """Update a data frame index with values from a list of key columns.

        :param df_content: The content data frame
        :param keys: The key columns
        """
        df_content.set_index(keys, drop=False, inplace=True)

    def _extract_core_keys(self, core_content, keys):
        """Get the key terms for a data frame.

        :param core_content: The content data frame
        :param keys: The keys that uniquely identify the record
        :return: A data frame indexed by the `id` column that contains the
                key elements for each record
        """
        columns = ['id']
        columns.extend(keys)
        df = core_content[columns]
        df.set_index('id', drop=True, inplace=True)
        return df

    def _cleanup_keys(self, core_index_keys):
        """Remove a key data frame (if it is a data frame)

        :param core_index_keys: The potential ket data frame
        """
        if isinstance(core_index_keys, pd.DataFrame):
            del core_index_keys

    def build_indexes(self):
        """Build unique indexes, using the key terms for both core and extensions
        """
        core_index_keys = self._extract_core_keys(self.core_content.df_content,
                                                  self.core_content.keys)
        for content in self.ext_content:
            self._add_ext_lookup_key(content.df_content, core_index_keys,
                                     self.core_content.keys, content.keys)

        self._cleanup_keys(core_index_keys)

        self._build_index_for_content(self.core_content.df_content, self.core_content.keys)

    def _add_core_key(self, df_content, core_df_content, core_keys):
        """Update the keys used to uniquely identify a record

        The first column is assumed to be the `coreid` or `id` field

        :param df_content: The (extension) data frame to update
        :param core_df_content: The core data frame
        :param core_keys: The keys to use to
        :return: The updated data frame
        """
        # assume that first column if the coreid or id field
        df_content.set_index(df_content.columns[0], drop=False, inplace=True)
        self._update_df(df_content, core_df_content, core_keys, core_keys)
        return df_content

    @record_diff_stat
    def _delete_content(self, content, delete_content):
        """Delete

        :param content: The existing data frame
        :param delete_content: The data frame to delete
        :return: The data frame with the list of deletions removed
        """
        content = self._filter_content(delete_content, content.df_content)
        return content

    def _delete_records(self, records_to_delete: CsvFileType):
        """Delete records from either a core or extension content frame

        :param records_to_delete: A CSV file of records to delete, keyed to the DwCA file
         """
        delete_content = self._combine_contents(
            records_to_delete.files, records_to_delete.csv_encoding, use_chunking=False)
        valid_delete_file = (all(col in delete_content.columns for col in records_to_delete.keys)
                             or len(delete_content) > 0)
        if not valid_delete_file:
            log.info("No records removed. Delete file does not contain any records "
                     "or it doesn't contain the columns: %s ", ','.join(records_to_delete.keys))
            return

        self._build_index_for_content(delete_content, records_to_delete.keys)
        dwca_content, core_or_ext = self._get_content(
                                MetaElementTypes.get_element(records_to_delete.type).row_type_ns)
        log.info("Removing records from %s", core_or_ext)
        if core_or_ext == CoreOrExtType.CORE:
            self.core_content.keys = records_to_delete.keys
            for ext in self.ext_content:
                ext.keys = records_to_delete.keys
            self.build_indexes()
        else:
            self._build_index_for_content(df_content=dwca_content.df_content,
                                          keys=records_to_delete.keys)

        log.info("Index built in %s. Starting deletion in core %s",
                 core_or_ext, records_to_delete.type)

        self.core_content.df_content = self._delete_content(content=dwca_content,
                                                            delete_content=delete_content)

        # Remove the extension records that are related to the core records that have been removed
        if core_or_ext == CoreOrExtType.CORE:
            for ext in self.ext_content:
                log.info("Removing records from ext: %s", ext.meta_info.type.name)
                ext.df_content = self._delete_content(content=ext.df_content,
                                                      delete_content=delete_content)

    def _add_ext_lookup_key(self, df_content, core_df_content, core_keys, keys):
        """Add a lookup key to a data frame

        :param df_content: The content data frame
        :param core_df_content: The core content data frame
        :param core_keys: The keys that uniquely identify the core record
        :param keys: The additional keys
        :return: The content data frame with additional indexes for the keys
        """
        # Core key is first level, hence need to be set first, then followed by extension key
        if not set(core_keys).issubset(df_content.columns.to_list()):
            self._add_core_key(df_content, core_df_content, core_keys)
            df_content.set_index(core_keys, inplace=True, drop=True)
        else:
            df_content.set_index(core_keys, inplace=True, drop=False)

        for key in keys:
            if key not in core_keys:
                df_content.set_index(key, inplace=True, drop=False, append=True)
        return df_content

    # Extension Sync
    def merge_contents(self, delta_dwca: Dwca, extension_sync: bool = False,
                       regen_ids: bool = False):
        """Merge the contents of this DwCA with a delta DwCA

        :param delta_dwca: The delta DwCA to apply
        :param extension_sync: refresh the extensions from delta dwca
                                if the occurrences exist in both
        :param regen_ids: Regenerate unique identifiers for the records
        """
        self.build_indexes()
        delta_dwca.build_indexes()

        for _, delta_content in enumerate(delta_dwca.ext_content):
            content, _ = self._get_content(delta_content.meta_info.type.row_type_ns)
            if content:
                if extension_sync:
                    self._delete_old_ext_records(content, self.core_content.df_content,
                                                 delta_dwca.core_content.df_content,
                                                 self.core_content.keys)
                # create a copy of list
                # Use keys other than coreid. Coreid should not be used as update keys if possible
                ext_keys = []
                ext_keys.extend(self.core_content.keys)
                update = False
                if len(content.keys) > 0:
                    ext_keys.extend(content.keys)
                    update = True
                content.df_content = self._merge_df_content(content, delta_content,
                                                            ext_keys, update)

            else:
                # Copy delta ext content into self ext content
                self.ext_content.append(delta_content)
                self._update_meta_fields(delta_content)

        self.core_content.df_content = self._merge_df_content(self.core_content, delta_dwca.core_content,
                                                              self.core_content.keys)

        if regen_ids:
            self._update_core_ids(self.core_content.df_content)
            for content in self.ext_content:
                content.df_content = self._update_extension_ids(
                    content.df_content, self.core_content.df_content, self.core_content.keys)

    def _get_content(self, name_space):
        """Get the content based on the row type namespace.

        :param name_space: The row type (a namespace URI)
        :return: A tuple of the content data frame and whether
                 it is a core or extension (None, None) if not found
        """
        if self.core_content.meta_info.type.row_type_ns == name_space:
            return self.core_content, CoreOrExtType.CORE

        for content in self.ext_content:
            if content.meta_info.type.row_type_ns == name_space:
                return content, CoreOrExtType.EXTENSION

        return None, None

    def _init_content(self):
        """Initialise a content frame

        :return: An empty data frame
        """
        return pd.DataFrame()

    def _extract_media(self, content, assoc_media_col: str):
        """Extract embedded associated media and place it in a media extension data frame
        Images from the media column, separated by a vertical bar or semicolon are extracted
        into a simple multimedia extension frame
        https://rs.gbif.org/extension/gbif/1.0/multimedia.xml with the media URL as the identifier.
        The associated media column is removed from the source frame.

        :param content: The content data frame
        :param assoc_media_col: The column that contains the associated media
        :return: The images data frame
        """
        image_df = pd.DataFrame(content[assoc_media_col])
        # filter off empty rows with empty value
        image_df = image_df[~image_df[assoc_media_col].isna()]
        if len(image_df) > 0:
            image_df = image_df.assign(identifier=image_df[assoc_media_col].
                                       str.split(r'[\\|;]')).explode('identifier')
            image_df.drop(columns=[assoc_media_col], inplace=True)
            content.drop(columns=[assoc_media_col], inplace=True)
            image_df['format'] = image_df['identifier'].map(
                lambda x: mimetypes.guess_type(x)[0] if mimetypes.guess_type(x)[0] else '')
            image_df['type'] = image_df['format'].map(
                lambda x: 'StillImage' if x.split('/')[0] == 'image' else 'Sound'
                if x.split('/')[0] == 'audio' else 'MovingImage' if x.split('/')[0] == 'video'
                else '')

        return image_df

    def convert_associated_media_to_extension(self):
        """Convert any embedded associated media terms in the core frame into a simple
        multimedia extension.

        :return: Either the new extension file or None for nothing done
        """
        core_fields = self._read_header(self.core_content.df_content)
        filtered_column = list(filter(lambda term:
                                      re.fullmatch('.*associatedMedia', term), core_fields))
        if len(filtered_column) > 0:
            log.info("Extracting associated media links")
            assoc_media_col = filtered_column[0]
            image_df = self._extract_media(self.core_content.df_content, assoc_media_col)
            if len(image_df) > 0:
                self._update_meta_fields(self.core_content)
                log.info("%s associated media extracted", str(len(image_df)))
                return CsvFileType(files=[image_df], type='multimedia', keys=image_df.index.names)

            log.info("Nothing to extract from associated media")

        return None

    def _combine_contents(self, contents: list, csv_encoding, use_chunking=False):
        """Combine the contents of a list of CSV files into a single content data frame.

        :param contents: The list of CSV files
        :param csv_encoding: The encoding to use
        :param use_chunking: Chunk large files while reading
        :return: The resulting data frame
        """
        if len(contents) > 0:
            if isinstance(contents[0], pd.DataFrame):
                return contents[0]

            df_content = self._init_content()
            for content in contents:
                df_content = self._add_new_rows(df_content,
                                                self._read_csv(content, ignore_header_lines=0,
                                                               csv_encoding_param=csv_encoding,
                                                               iterator=use_chunking))

            log.info("Extracted total of %d records from %s",
                     self.count_stat(df_content), ','.join(contents))
            # Drop rows where all the values are duplicates
            df_content.drop_duplicates(inplace=True)
            log.debug("Extracted %d unique rows from csv %s",
                      len(df_content), ','.join(contents))
            return df_content

        raise ValueError('content is empty')

    def __check_csv_info_value(self, csv_info: CsvFileType, col: str):
        """Look for a column in a CSV file

        :param csv_info: The CSV file
        :param col: The column name
        :return: Either column information or False for not found
        """
        csv_info_dict = asdict(csv_info)
        if col in csv_info_dict:
            return csv_info_dict[col]
        return False

    def check_duplicates(self, content_keys_df, keys, error_file=None):
        """Check a content frame for duplicate keys

        :param content_keys_df: The content frame to check
        :param keys: The key columns
        :param error_file: A file to write problem records to
        :return: True if there are no duplicates, False otherwise
        """

        def report_error(content, keys, message, condition, error_file=None):
            log.error("%s found in keys %s", message, keys)
            log.error("\n%s count\n%s", message, condition.sum())
            log.error("\n%s",content.loc[condition.values, keys].index.tolist())
            if error_file:
                content.loc[condition.values, keys].to_csv(error_file, index=False)

        checks_status: bool = True
        if len(keys) > 0:
            empty_values_condition = content_keys_df.isnull()
            if empty_values_condition.values.any():
                report_error(content_keys_df, keys, "Empty Values", empty_values_condition)
                checks_status = False

            # check incase-sensitive duplicates
            def to_lower(df):
                df = df.apply(lambda x: x.str.lower() if x.dtype == "object" else x)
                return df

            df_keys = to_lower(content_keys_df)
            duplicate_condition = df_keys.duplicated(keep='first')
            if duplicate_condition.values.any():
                report_error(content_keys_df, keys, "Duplicate Values",
                             duplicate_condition, error_file)
                checks_status = False

        return checks_status

    def _extract_keys(self, df_content, keys):
        """Get the key columns for a data frame

        :param df_content: The content data frame
        :param keys: The key columns
        :return: A data frame containing only the key values
        """
        return df_content[keys]

    def _validate_columns(self, content):
        """Validate the columns in content
            Validate the column header if any of it contains Unnamed header.
            This usually happens if a csv has empty column. Pandas automatically
            assigns the column header with a column name called Unnamed:

        :param content: The content
        :return: True if all columns have a valid name,
                False if a name is blank or column contain some unnamed header
        """
        headers = self._read_header(content.df_content)
        if sum(not c or c.isspace() for c in headers) > 0:
            log.error("Some column headers are blank")
            return False

        if content.df_content.columns.str.contains('^unnamed:', case=False).any():
            log.error("One or more column is unnamed. "
                      "This usually happens if there are empty column in the csv")
            return False

        return True

    def validate_content(self, content_type_to_validate: list[str] = None, error_file: str = None):
        """Validate the content of the DwCA

        - No duplicate record keys
        - Valid columns
        - No duplicate columns

        :param error_file: A file to record errors
        :return: True if the DwCA is value, False otherwise
        """

        if not content_type_to_validate:
            content_type_to_validate = [self.core_content.meta_info.type.name]

        for content_type in content_type_to_validate:
            content, _ = self._get_content(MetaElementTypes.get_element(content_type).row_type_ns)
            keys_df = self._extract_keys(content.df_content, content.keys)

            if not self.check_duplicates(keys_df, content.keys, error_file):
                return False

            if not self._validate_columns(content):
                return False

            dup_cols = self._find_duplicate_columns(content)
            if len(dup_cols) > 0:
                return False

        return True

    def extract_csv_content(self, csv_info: Union[CsvFileType, DataFrameType],
                            core_ext_type: CoreOrExtType):
        """Read the files from a CSV description into a content frame and include it in the Dwca.

        :param csv_info: The CSV file(s)
        :param core_ext_type: Whether this is a core or extension content frame
        """
        if isinstance(csv_info, DataFrameType):
            csv_content = csv_info.df
        else:
            csv_content = self._combine_contents(csv_info.files, csv_info.csv_encoding)

        # Use default occurrenceID if not provided
        keys = csv_info.keys if self.__check_csv_info_value(csv_info, 'keys') else 'occurrenceID'
        if core_ext_type == CoreOrExtType.CORE:
            self._update_core_ids(csv_content)
            self._build_index_for_content(csv_content, keys)
        else:
            csv_content = self._update_extension_ids(
                csv_content, self.core_content.df_content, keys)

        if csv_info.associated_files_loc:
            self._update_associated_files([csv_info.associated_files_loc])

        meta_type = MetaElementTypes.get_element(csv_info.type)
        meta_element_info = MetaElementInfo(
            core_or_ext_type=core_ext_type, type=meta_type,
            csv_encoding=self.defaults_prop.csv_encoding, ignore_header_lines='1')
        content = DfContent(df_content=csv_content, meta_info=meta_element_info)
        self._update_meta_fields(content)

        if core_ext_type == CoreOrExtType.CORE:
            content.keys = keys
            self.core_content = content
        else:
            self.ext_content.append(content)

    def _to_csv(self, df: pd.DataFrame, meta_info: MetaElementInfo,
                write_header: bool = False) -> str:
        """Convert a data frame into CSV

        :param df: The data frame
        :param meta_info: Information about the columns and expected encoding
        :param write_header: Write a header to the top of CSV
        :return: The CSV content as a string
        """
        content = df.to_csv(
            lineterminator='\r\n' if meta_info.csv_encoding.csv_eol == '\\r\\n'
                            else meta_info.csv_encoding.csv_eol,
            sep=meta_info.csv_encoding.csv_delimiter,
            quotechar=meta_info.csv_encoding.csv_text_enclosure,
            escapechar=meta_info.csv_encoding.csv_escape_char,
            header=write_header,
            quoting=csv.QUOTE_MINIMAL,
            index=False)
        return content

    def _write_df_content_to_zip_file(self, dwca_zip: ZipFile, content: DfContent):
        """Add a content frame to a zip file

        :param dwca_zip: The zip file to write to
        :param content: The content frame
        """
        def str2bool(v):
            return v.lower() in ("yes", "true", "t", "1")

        header = str2bool(content.meta_info.ignore_header_lines)
        dwca_zip.writestr(content.meta_info.file_name,
                          self._to_csv(content.df_content, content.meta_info, header))

    def _write_associated_files(self, dwca_zip: ZipFile):
        """Write any additional files to a zip file

        :param dwca_zip: The zip file to write to
        """
        for file in self.embedded_files:
            dwca_zip.write(file, file.name)

    def write_dwca(self, output_dwca_path: str):
        """Write a full DwCA to a zip file
        Any parent directories needed are created during writing.

        :param output_dwca_path: The file path to write the .zip file to
        """
        dwca_path = Path(output_dwca_path)
        dwca_path.parent.mkdir(parents=True, exist_ok=True)
        with ZipFile(output_dwca_path, 'w', allowZip64=True,
                     compression=zipfile.ZIP_DEFLATED) as dwca_zip:
            self._write_df_content_to_zip_file(dwca_zip=dwca_zip, content=self.core_content)
            for ext in self.ext_content:
                self._write_df_content_to_zip_file(dwca_zip=dwca_zip, content=ext)
            dwca_zip.writestr(self.defaults_prop.meta_xml_filename, str(self.meta_content))
            if self.eml_content:
                dwca_zip.writestr(self.defaults_prop.eml_xml_filename, self.eml_content)
            self._write_associated_files(dwca_zip=dwca_zip)
            dwca_zip.close()
        log.info("Dwca written to: %s", output_dwca_path)

    def _read_csv(self,
                  csv_file: str | io.TextIOWrapper,
                  csv_encoding_param: CSVEncoding = MISSING,
                  columns: list = None,
                  ignore_header_lines: int = 0,
                  iterator: bool = False,
                  chunksize: int = 100,
                  nrows: int = 0) -> Union[pd.DataFrame, parsers.TextFileReader]:
        """Read a CSV file and convert it into a data frame

        :param csv_file:  The file path
        :param csv_encoding_param: The encoding to use (defaults to the default CSV encoding)
        :param columns: The columns to read (defaults to all columns)
        :param ignore_header_lines: If columns are passed, the first line is treated as data,
            hence pass ignore_header_lines should be set to 1 if first line is header in the csv.
            If columns are not passed, ignore_header line should be 0
        :param iterator: Return an iterator, rather than a data frame (False by default)
        :param chunksize: The number of records to chunk
        :param nrows: The number of rows to read (all by default)
        :return: Either a data frame or a reader, depending on the iterator parameter
        """
        if csv_encoding_param is MISSING:
            csv_encoding_param = self.defaults_prop.csv_encoding

        # Note: having lineterminator as \n leaves \r in the column text if \r\n is present.
        #       pandas read_csv cannot support passing in \r\n, omitting lineterminator seem to
        #       work properly passing in escapechar as double-quote into pandas does not
        #       work with csv that have double quotes around every field, only set escapechar,
        #       if it is other than double-quotes.
        escape_char = csv_encoding_param.csv_escape_char \
                        if csv_encoding_param.csv_escape_char != '"' else None
        quote_char = csv_encoding_param.csv_text_enclosure \
                        if csv_encoding_param.csv_text_enclosure != '' else '"'
        line_terminator = csv_encoding_param.csv_eol \
            if (csv_encoding_param.csv_eol not in ['\r\n', '\n', '\\r\\n']) \
            else None

        try:
            ret_val = pd.read_csv(csv_file, delimiter=csv_encoding_param.csv_delimiter,
                                  escapechar=escape_char,
                                  quotechar=quote_char,
                                  lineterminator=line_terminator,
                                  names=columns,
                                  skiprows=ignore_header_lines,  # skip first n lines
                                  skip_blank_lines=True,
                                  dtype='str',
                                  index_col=False,
                                  chunksize=chunksize if iterator else None,
                                  iterator=iterator,
                                  nrows=nrows if nrows > 0 else None)

            if isinstance(ret_val, pd.DataFrame):
                # Drop rows where all the columns are Nan
                ret_val.dropna(how="all", inplace=True)
                log.debug("Extracted %d rows from csv %s", len(ret_val), csv_file)

            return ret_val

        except EmptyDataError:
            log.error(f"The expected columns: %s are not present in the {csv_file}. "
                      f"The file may be empty",','.join(columns))
            return pd.DataFrame()
