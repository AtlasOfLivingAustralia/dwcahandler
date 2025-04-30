"""
Module contains the Darwin Core operations

"""

from __future__ import annotations

import csv
import io
import logging
import mimetypes
import re
import uuid
from io import BytesIO
import zipfile
from dataclasses import MISSING, asdict, dataclass, field
from pathlib import Path
from typing import Union
from zipfile import ZipFile
import pandas as pd
from numpy import nan
from pandas.errors import EmptyDataError
from pandas.io import parsers
from dwcahandler.dwca import (BaseDwca, CoreOrExtType, CSVEncoding,
                              ContentData, Defaults, Eml, Terms, get_keys,
                              MetaDwCA, MetaElementInfo, MetaElementTypes,
                              MetaElementAttributes, Stat, record_diff_stat,
                              ValidationError)

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
    dwca_file_loc: Union[str, BytesIO] = field(default='./')
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

    def _update_core_ids(self, core_df) -> str:
        """Generate core identifiers for a core data frame.

        UUID identifiers are generated for each row in the core data frame.
        These identifiers can be used to link core and extension records when no immediately
        useful identifier is available in the source data.

        :param core_df: The data frame to generate identifiers for
        return id field
        """
        if self.defaults_prop.MetaDefaultFields.ID not in core_df.columns.to_list():
            core_df.insert(0, self.defaults_prop.MetaDefaultFields.ID, core_df.apply(lambda _: uuid.uuid4(), axis=1), False)
            return self.defaults_prop.MetaDefaultFields.ID
        else:
            raise ValueError("core df should not contain id column")

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

    def _update_extension_ids(self, csv_content: pd.DataFrame, core_df_content: pd.DataFrame,
                              link_col: list) -> (pd.DataFrame, str):
        """Update the extension tables with (usually generated) identifiers
            from a core data frame.

        DwCAs only allow a single link identifier.
        If the link between the core and extension requires multiple fields, then an identifier
        column needs to be generated and linked across both data frames.

        :param csv_content: The extension to update
        :param core_df_content: The core data frame
        :param link_col: The columns that link the extension to the core
        :return a tuple containing extension data frame containing the core id and the core id field
        """
        ext_core_id_field: str = 'coreid'

        if ext_core_id_field in csv_content:
            csv_content.pop(ext_core_id_field)

        # Having link_col as index and column raises ambiguous error in merge
        if (set(link_col).issubset(set(csv_content.columns.to_list())) and
                set(link_col).issubset(set(csv_content.index.names))):
            csv_content.reset_index(inplace=True, drop=True)

        csv_content = csv_content.merge(core_df_content.loc[:, self.defaults_prop.MetaDefaultFields.ID],
                                        left_on=link_col,
                                        right_on=link_col, how='inner')

        if self.defaults_prop.MetaDefaultFields.ID in csv_content.columns.to_list():
            unmatched_content = csv_content[csv_content[self.defaults_prop.MetaDefaultFields.ID].isnull()]
            unmatched_content = unmatched_content.drop(columns=[self.defaults_prop.MetaDefaultFields.ID])
            if len(unmatched_content) > 0:
                log.info("There are orphaned keys in extension file")
                pd.set_option("display.max_columns", 7)
                pd.set_option('display.max_colwidth', 15)
                pd.set_option('display.max_rows', 10)
                log.info("\n%s", unmatched_content)
            csv_content = csv_content[~csv_content[self.defaults_prop.MetaDefaultFields.ID].isnull()]
            col = csv_content.pop(self.defaults_prop.MetaDefaultFields.ID)
            csv_content.insert(0, col.name, col)
            csv_content.rename(columns={self.defaults_prop.MetaDefaultFields.ID: ext_core_id_field}, inplace=True)
            return csv_content, ext_core_id_field
        else:
            raise ValueError("Something is not right. The core id failed to be created")

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

    def extract_dwca(self, extra_read_param: dict = None, exclude_ext_files: list = None):
        """Read a DwCA file into this object.
        The archive is expected to be in zip file form, located at the `dwca_file_loc` attribute.
        The content and meta-information are initialised from the archive.

        :param extra_read_param: additional read param to use when reading
        :param exclude_ext_files: Ignore the following file names
        """
        def convert_values(v):
            invalid_values = self.defaults_prop.translate_table.keys()
            return self.defaults_prop.translate_table[v] if v in invalid_values else v

        def _find_fields_with_zero_idx(meta_element_fields: list):
            for elm_field in meta_element_fields:
                if elm_field.index == "0":
                    return elm_field
            return None

        def _add_first_id_field_if_exists(meta_element: MetaElementAttributes):
            zero_index_exist = _find_fields_with_zero_idx(meta_element.fields)
            if meta_element.core_id and meta_element.core_id.index and not zero_index_exist:
                return [self.defaults_prop.MetaDefaultFields.ID] if (
                        meta_element.meta_element_type.core_or_ext_type == CoreOrExtType.CORE) \
                    else [self.defaults_prop.MetaDefaultFields.CORE_ID]
            else:
                return []

        with ZipFile(self.dwca_file_loc, 'r') as zf:

            files = zf.namelist()

            log.info("Reading from %s. Zip file size is %i, containing files: %s",
                     self.dwca_file_loc, zf.start_dir, ",".join(zf.namelist()))
            with io.TextIOWrapper(zf.open(self.defaults_prop.meta_xml_filename)) as meta_xml:
                self.meta_content.read_meta_file(meta_xml)
                meta_xml.close()

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
                    dwc_headers = _add_first_id_field_if_exists(meta_elm)
                    dwc_headers.extend([f.field_name for f in meta_elm.fields if f.index is not None])
                    duplicates = [i for i in set(dwc_headers) if dwc_headers.count(i) > 1]
                    if len(duplicates) > 0:
                        raise ValueError(f"Duplicate columns {duplicates} specified in the "
                                         f"metadata for {csv_file_name}")
                    csv_encoding = {key: convert_values(value) for key, value in
                                    asdict(meta_elm.meta_element_type.csv_encoding).items()}
                    csv_content = self._read_csv(
                        csv_file, columns=dwc_headers,
                        csv_encoding_param=CSVEncoding(**csv_encoding),
                        ignore_header_lines=int(meta_elm.meta_element_type.ignore_header_lines),
                        extra_param=extra_read_param)
                    if meta_elm.meta_element_type.core_or_ext_type == CoreOrExtType.CORE:
                        self.core_content = self._set_content(csv_content,
                                                              meta_elm.meta_element_type)
                    else:
                        self.ext_content.append(self._set_content(csv_content,
                                                                  meta_elm.meta_element_type))
                    csv_file.close()

            zf.close()

    def _add_new_columns(self, df_content, delta_df_content, keys):
        """Add additional columns to a data frame if they're not part of the keys

        New columns are initialised to vectors of NaN

        :param df_content: The base data frame content
        :param delta_df_content: The data frane content to add
        :param keys: keys used for merging
        """
        df_columns = df_content.columns.to_list()
        delta_df_columns = delta_df_content.columns.to_list()
        new_columns = list(set(delta_df_columns) - set(df_columns))
        new_columns = list(set(new_columns) - set(keys))
        if len(new_columns) > 0:
            # Set to empty string instead of nan to resolve warning message
            # see https://pandas.pydata.org/pdeps/0006-ban-upcasting.html
            df_content[new_columns] = ""

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
        non_update_column = list(self.defaults_prop.MetaDefaultFields)
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

    def _delete_old_ext_records(self, content, core_content, delta_core_content):
        """Drop all extension rows where core records are exist in both content and delta

        :param content: The extension
        :param core_content: The core records
        :param delta_core_content: The delta update
        """
        core_exist = self._find_records_exist_in_both(core_content, delta_core_content)
        exist = content.df_content.droplevel(content.keys).index.isin(core_exist)
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

    def set_keys(self, keys: dict = None, strict: bool = False):
        """Set unique identifier keys.

        :param keys: The dict of keys for content
        :param strict: The extension keys must be set if the default keys is defined for the extension.
                        This is necessary for merging extension contents
        :return: The keys which have been set for the content
        """
        set_keys = {}
        if keys and len(keys) > 0:
            for k, v in keys.items():
                contents = self.get_content(class_type=k)
                # If found then set the key for the content
                for dwca_content, _ in contents:
                    key_list = [v] if isinstance(v, str) else v
                    col_term = []
                    for a_key in key_list:
                        # this is in case a_key is url form for eg: http://rs.gbif.org/terms/1.0/gbifID
                        if a_key not in dwca_content.df_content.columns.tolist():
                            col_term.append(Terms.extract_term(a_key))
                        else:
                            col_term.append(a_key)
                    dwca_content.keys = col_term
                    set_keys[k] = col_term

        if strict:
            # Set default key for remaining extension content which has not been set
            for content in self.ext_content:
                if content.meta_info.type and len(content.keys) == 0:
                    keys = get_keys(class_type=content.meta_info.type)
                    if len(keys) > 0:
                        content.keys = keys
                        set_keys[content.meta_info.type] = keys

        return set_keys

    def _update_meta_fields(self, content: DfContent, key_field: str = None):
        """Update meta content fields by reading the content frame"""
        fields = self._read_header(content.df_content)
        self.meta_content.update_meta_element(meta_element_info=content.meta_info, fields=fields,
                                              index_field=key_field)

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
        new_columns = self._add_new_columns(df_content, delta_df_content, keys)

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

    def _build_index_for_content(self, df_content: pd.DataFrame, keys: list):
        """Update a data frame index with values from a list of key columns.

        :param df_content: The content data frame
        :param keys: The key columns
        """
        df_content.set_index(keys, drop=False, inplace=True)

    def _extract_core_keys(self, core_content: pd.DataFrame, keys: list, id_column: str):
        """Get the key terms for a data frame.

        :param core_content: The content data frame
        :param keys: The keys that uniquely identify the record
        :param id_column: Column name used for id
        :return: A data frame indexed by the `id` column that contains the
                key elements for each record
        """
        columns = keys.copy()
        if id_column not in keys:
            columns.append(id_column)
        df = pd.DataFrame()
        if all(col in core_content.columns for col in columns):
            df = core_content[columns].copy()
            df.set_index(id_column, drop=False, inplace=True)
        else:
            raise ValueError(f"Keys does not exist in core content {''.join(keys)}")
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

        def __get_coreid_column(content: DfContent):
            """
            Get the column name of id or coreid
            :param content: Content to find the id or coreid column
            :return: The column name if found
            """
            for elm in self.meta_content.meta_elements:
                if elm.meta_element_type.file_name == content.meta_info.file_name:
                    coreid_idx = elm.core_id.index
                    for a_field in elm.fields:
                        if a_field.index == coreid_idx:
                            return a_field.field_name
                    return Defaults.MetaDefaultFields.ID if content.meta_info.core_or_ext_type == CoreOrExtType.CORE \
                        else Defaults.MetaDefaultFields.CORE_ID
            return None

        if len(self.ext_content) > 0:
            id_column = __get_coreid_column(self.core_content)
            core_index_keys = self._extract_core_keys(self.core_content.df_content, self.core_content.keys, id_column)
            for content in self.ext_content:
                coreid_column = __get_coreid_column(content)
                if coreid_column:
                    # Make sure coreid columns are populated by filtering off empty core ids.
                    log.info("content %s contains %i records before filtering empty coreid",
                             content.meta_info.file_name, len(content.df_content))
                    content.df_content = content.df_content[content.df_content[coreid_column].notna()]
                    log.info("content %s contains %i records after filtering empty coreid",
                             content.meta_info.file_name, len(content.df_content))
                    content.df_content = content.df_content[content.df_content[coreid_column].isin(core_index_keys.index)]
                    log.info("content %s contains %i records after filtering unlinked coreids",
                             content.meta_info.file_name, len(content.df_content))

                    self._add_ext_lookup_key(content.df_content, core_index_keys,
                                             self.core_content.keys, content.keys, coreid_column)

            self._cleanup_keys(core_index_keys)

        self._build_index_for_content(self.core_content.df_content, self.core_content.keys)

    def _add_core_key(self, df_content: pd.DataFrame, core_df_content: pd.DataFrame, core_keys: list,
                      coreid_column: str):
        """Update the keys used to uniquely identify a record

        The first column is assumed to be the `coreid` or `id` field

        :param df_content: The (extension) data frame to update
        :param core_df_content: The core data frame containing the keys (derived from core content)
        :param core_keys: The keys which need to be added to the df_content
        :param coreid_column: Column name used for coreid
        :return: The updated data frame
        """
        df_content.set_index(coreid_column, drop=False, inplace=True)
        self._update_df(df_content, core_df_content, core_keys, core_keys)
        df_content.reset_index(inplace=True, drop=True)
        return df_content

    @record_diff_stat
    def _delete_content(self, content, delete_content):
        """Delete

        :param content: The existing data frame where the records need to be deleted
        :param delete_content: The data frame containing the keys
        :return: The data frame with the records removed
        """
        content = content.df_content[~content.df_content.index.isin(delete_content.index)]
        return content

    def delete_records(self, records_to_delete: ContentData):
        """Delete records from either a core or extension content frame

        :param records_to_delete: A CSV file of records to delete, keyed to the DwCA file
         """
        delete_content = pd.DataFrame()
        if isinstance(records_to_delete.data, pd.DataFrame):
            delete_content = records_to_delete.data.copy(deep=True)
        else:
            delete_content = self._combine_contents(records_to_delete.data, records_to_delete.csv_encoding,
                                                    use_chunking=False)
        valid_delete_file = (all(col in delete_content.columns for col in records_to_delete.keys)
                             or len(delete_content) > 0)
        if not valid_delete_file:
            log.info("No records removed. Delete file does not contain any records "
                     "or it doesn't contain the columns: %s ", ','.join(records_to_delete.keys))
            return

        self._build_index_for_content(delete_content, records_to_delete.keys)
        contents = self.get_content(class_type=records_to_delete.type)

        for dwca_content, core_or_ext in contents:
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
                    ext.df_content = self._delete_content(content=ext,
                                                          delete_content=delete_content)

    def _add_ext_lookup_key(self, df_content: pd.DataFrame, core_df_content: pd.DataFrame, core_keys: list,
                            keys: list, coreid_column: str):
        """Add a lookup key to a data frame

        :param df_content: The content data frame
        :param core_df_content: The core content data frame
        :param core_keys: The keys that uniquely identify the core record
        :param keys: The additional keys for extension (for eg: identifier for multimedia extension)
        :param coreid_column: Column used for coreid
        :return: The content data frame with indexes for the keys
        """
        existing_col = []
        for a_key in core_keys:
            if a_key in df_content.columns.to_list():
                existing_col.append(a_key)

        # Add the key column from core content that is not part of the coreid
        if len(core_keys) > 0:
            self._add_core_key(df_content, core_df_content, core_keys, coreid_column)

        for i, a_key in enumerate(core_keys):
            drop_flag = False if a_key in existing_col else True
            append_flag = True if i > 0 else False
            df_content.set_index(a_key, inplace=True, drop=drop_flag, append=append_flag)

        for key in keys:
            if key not in core_keys:
                df_content.set_index(key, inplace=True, drop=False, append=True)
        return df_content

    def merge_contents(self, delta_dwca: Dwca, extension_sync: bool = False,
                       match_by_filename: bool = False):
        """Merge the contents of this DwCA with a delta DwCA

        :param delta_dwca: The delta DwCA to apply
        :param extension_sync: if True, remove existing extension and refresh the extensions from delta dwca
                                if the occurrences exist in both
        :param match_by_filename: Match by filename of contents apart from the content types.
        This is particularly useful if a dwca contains more than one content of same type
        """
        self.build_indexes()
        delta_dwca.build_indexes()

        for _, delta_content in enumerate(delta_dwca.ext_content):
            contents = self.get_content(class_type=delta_content.meta_info.type,
                                        file_name=delta_content.meta_info.file_name if match_by_filename else "")
            for content, _ in contents:
                if extension_sync:
                    self._delete_old_ext_records(content, self.core_content.df_content,
                                                 delta_dwca.core_content.df_content)

                content.df_content = self._merge_df_content(content=content,
                                                            delta_content=delta_content,
                                                            keys=self.core_content.keys)

            if len(contents) == 0:
                # Copy delta ext content into self ext content
                self.ext_content.append(delta_content)
                self._update_meta_fields(delta_content)

        self.core_content.df_content = self._merge_df_content(content=self.core_content,
                                                              delta_content=delta_dwca.core_content,
                                                              keys=self.core_content.keys)

    def get_content(self, class_type: MetaElementTypes = None, name_space: str = None, file_name: str = None) -> list:
        """Get the content based on the class type, row type namespace and optional file name

        :param class_type: class_type MetaElementTypes class
        :param name_space: The row type (a namespace URI) if it contains value
        :param file_name: file_name to match if it contains value
        :return: A list of tuples containing the content data frame and
                 core or extension type
        """
        def check_content(current_content, class_type_to_match, name_space_to_match):
            if file_name and current_content.meta_info.file_name != file_name:
                return False

            if ((class_type_to_match and current_content.meta_info.type == class_type_to_match) or
                    (name_space_to_match and current_content.meta_info.type.value == name_space_to_match)):
                return True
            return False

        contents = []

        if check_content(self.core_content, class_type_to_match=class_type, name_space_to_match=name_space):
            contents.append((self.core_content, CoreOrExtType.CORE))

        for content in self.ext_content:
            if check_content(content, class_type_to_match=class_type, name_space_to_match=name_space):
                contents.append((content, CoreOrExtType.EXTENSION))

        return contents

    def add_multimedia_info_to_content(self, multimedia_content: DfContent):
        """
        Attempt to populate the format and type from the url provided in the multimedia ext if none is provided
        :param multimedia_content: Multimedia content derived from the extension of this Dwca class object
        """
        def get_media_format_prefix(media_format: str):
            media_format_prefixes = ["image", "audio", "video"]
            if media_format and isinstance(media_format, str) and '/' in media_format:
                prefix = media_format.split('/')[0]
                if prefix in media_format_prefixes:
                    return prefix

            return None

        def get_media_type(media_format: str):
            media_type = None
            m_type = get_media_format_prefix(media_format)
            if m_type == 'image':
                media_type = 'StillImage'
            elif m_type == 'audio':
                media_type = 'Sound'
            elif m_type == 'video':
                media_type = 'MovingImage'
            if media_type is None and media_format:
                log.warning("Unknown media type for format %s", media_format)

            return media_type

        def get_multimedia_format_type(row: dict):
            url = row['identifier']
            media_format = None
            if url:
                try:
                    mime_type = mimetypes.guess_type(url)
                    if mime_type and len(mime_type) > 0 and mime_type[0]:
                        media_format = mime_type[0]
                except Exception as error:
                    log.error("Error getting mimetype from url %s: %s", url, error)

            media_type = ''
            if 'type' not in row or not row['type'] or row['type'] is nan:
                media_type = get_media_type(media_format)
            else:
                media_type = row['type']

            row['format'] = media_format if media_format else None
            row['type'] = media_type if media_type else None
            return row

        if len(multimedia_content.df_content) > 0:

            multimedia_df = multimedia_content.df_content

            if 'format' in multimedia_df.columns:
                multimedia_without_format = multimedia_df[multimedia_df['format'].isnull()]
                if len(multimedia_without_format) > 0:
                    multimedia_without_format = multimedia_without_format.apply(
                                                                    lambda row: get_multimedia_format_type(row),
                                                                    axis=1)
                    multimedia_df.update(multimedia_without_format)
            else:
                multimedia_df = multimedia_df.apply(lambda row: get_multimedia_format_type(row), axis=1)

            multimedia_without_type = multimedia_df
            # In case if the type was not populated from format
            if 'type' in multimedia_df.columns:
                multimedia_without_type = multimedia_df[multimedia_df['type'].isnull()]
                multimedia_without_type = multimedia_without_type[multimedia_without_type['format'].notnull()]

            if len(multimedia_without_type) > 0:
                multimedia_without_type.loc[:, 'type'] = multimedia_without_type['format'].map(lambda x: get_media_type(x))
                multimedia_df.update(multimedia_without_type)

            # Only update if there are additional info added
            if len(multimedia_df.columns) > len(multimedia_content.df_content.columns):
                multimedia_content.df_content = multimedia_df
                self._update_meta_fields(content=multimedia_content)

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
        cols = []
        if len(self.core_content.df_content.index.names) > 0:
            cols = self.core_content.keys.copy()
            cols.append(assoc_media_col)
            image_df = pd.DataFrame(content[cols])
            # filter off empty rows with empty value
            image_df = image_df[~image_df[assoc_media_col].isna()]
            if len(image_df) > 0:
                image_df = image_df.assign(identifier=image_df[assoc_media_col].
                                           str.split(r'[\\|;]')).explode('identifier')
                image_df.drop(columns=[assoc_media_col], inplace=True)
                content.drop(columns=[assoc_media_col], inplace=True)
            return image_df
        return pd.DataFrame()

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
                image_df.drop_duplicates(inplace=True)
                self._update_meta_fields(content=self.core_content, key_field=self.core_content.keys[0])
                log.info("%s associated media extracted", str(len(image_df)))
                multimedia_keys = self.core_content.keys.copy()
                multimedia_keys.append("identifier")
                return ContentData(data=image_df, type=MetaElementTypes.MULTIMEDIA,
                                   keys=multimedia_keys)

            log.info("Nothing to extract from associated media")

        return None

    def _combine_contents(self, contents: list, csv_encoding, extra_read_param: dict = None, use_chunking=False):
        """Combine the contents of a list of CSV files into a single content data frame.

        :param contents: The list of CSV files
        :param csv_encoding: The encoding to use
        :param use_chunking: Chunk large files while reading
        :return: The resulting data frame
        """
        if len(contents) > 0:
            if isinstance(contents[0], pd.DataFrame):
                return contents[0].copy(deep=True)

            df_content = pd.DataFrame()
            for content in contents:
                df_content = self._add_new_rows(df_content,
                                                self._read_csv(content, ignore_header_lines=0,
                                                               csv_encoding_param=csv_encoding,
                                                               iterator=use_chunking,
                                                               extra_param=extra_read_param))

            log.info("Extracted total of %d records from %s",
                     self.count_stat(df_content), ','.join(contents))
            # Drop rows where all the values are duplicates
            df_content.drop_duplicates(inplace=True)
            log.debug("Extracted %d unique rows from csv %s",
                      len(df_content), ','.join(contents))
            return df_content

        raise ValueError('content is empty')

    def __report_error(self, content_type: MetaElementTypes, message: ValidationError,
                       error_values: list, rows: list, error_df: pd.DataFrame = None):
        """Update error report if this is set
        :param content_type type of content
        :param message Error message
        :param error_values Values that fail validation
        :param rows Row number that cause the failed validation. Starts with 0
        """
        if isinstance(error_df, pd.DataFrame):
            error_report = {"Content": content_type.value,
                            "Message": message.value,
                            "Error": str(error_values),
                            "Row": str(rows)}
            error_df.loc[len(error_df)] = error_report
            return error_df

    def check_duplicates(self, content_type: MetaElementTypes, content_keys_df: pd.DataFrame,
                         keys: list, error_df: pd.DataFrame = None):
        """Check a content frame for duplicate keys

        :param content_type: Content Type where the validation is occurring
        :param content_keys_df: The content frame to check
        :param keys: The key columns
        :param error_df: Report dataframe
        :return: True if there are no duplicates, False otherwise
        """
        checks_status: bool = True
        if len(keys) > 0:
            empty_values_condition = content_keys_df.isnull()
            if empty_values_condition.values.any():
                log.error("Empty values found in %s. Total rows affected: %s", keys,
                          empty_values_condition.sum().sum())
                log.error("Empty values found in dataframe row: %s",
                          content_keys_df.index[empty_values_condition.all(axis=1)].tolist())

                self.__report_error(content_type=content_type,
                                    message=ValidationError.EMPTY_KEYS,
                                    error_values=[None],
                                    rows=content_keys_df.index[empty_values_condition.all(axis=1)].tolist(),
                                    error_df=error_df)
                checks_status = False

            # check incase-sensitive duplicates
            def to_lower(df):
                df = df.apply(lambda x: x.str.lower() if x.dtype == "object" else x)
                return df

            df_keys = to_lower(content_keys_df)
            duplicate_condition = df_keys.duplicated(keep='first')
            if duplicate_condition.values.any():
                log.error("Duplicate %s found. Total rows affected: %s", keys, duplicate_condition.sum())
                log.error("Duplicate values: %s", pd.unique(content_keys_df[duplicate_condition].stack()))
                self.__report_error(content_type=content_type,
                                    message=ValidationError.EMPTY_KEYS,
                                    error_values=list(pd.unique(content_keys_df[duplicate_condition].stack())),
                                    rows=content_keys_df.index[duplicate_condition].tolist(),
                                    error_df=error_df)
                checks_status = False

        return checks_status

    def _extract_keys(self, df_content: pd.DataFrame, keys: list):
        """Get the key columns for a data frame

        :param df_content: The content data frame
        :param keys: The key columns
        :return: A data frame containing only the key values
        """
        return df_content[keys]

    def _validate_columns(self, content_type: MetaElementTypes, content: DfContent, error_df: pd.DataFrame):
        """Validate the columns in content
            Validate the column header if any of it contains Unnamed header.
            This usually happens if a csv has empty column. Pandas automatically
            assigns the column header with a column name called Unnamed:

        :param content_type The Content Type
        :param content: The content itself
        :param error_df: The report dataframe containing the error validation
        :return: True if all columns have a valid name,
                False if a name is blank or column contain some unnamed header
        """
        headers = self._read_header(content.df_content)
        if sum(not c or c.isspace() for c in headers) > 0:
            log.error("Some column headers are blank")
            self.__report_error(content_type=content_type,
                                message=ValidationError.UNNAMED_COLUMNS,
                                error_values=[None],
                                rows=[None],
                                error_df=error_df)
            return False

        if content.df_content.columns.str.contains('^unnamed:', case=False).any():
            log.error("One or more column is unnamed. "
                      "This usually happens if there are empty column in the csv")
            self.__report_error(content_type=content_type,
                                message=ValidationError.UNNAMED_COLUMNS,
                                error_values=["^unnamed"],
                                rows=[None],
                                error_df=error_df)
            return False

        return True

    def validate_content(self, content_to_validate: dict = None, error_df: pd.DataFrame = None):
        """Validate the content of the DwCA. Validates core content by default

        - No duplicate record keys
        - Valid columns

        :param content_to_validate: content to validate
        :param error_df: A file to record errors
        :return: True if the DwCA is value, False otherwise
        """

        set_to_validate = {self.core_content.meta_info.type: self.core_content.keys}
        if content_to_validate:
            for class_type, content_keys in content_to_validate.items():
                if not (class_type == self.core_content.meta_info.type and
                        set(content_keys) == set(self.core_content.keys)):
                    set_to_validate[class_type] = content_keys

        validation_success = True
        for class_type, key in set_to_validate.items():
            contents = self.get_content(class_type=class_type)
            for content, _ in contents:
                validation_content_success = True
                keys_df = self._extract_keys(content.df_content, content.keys)

                if not self.check_duplicates(class_type, keys_df, content.keys, error_df):
                    log.error("Validation failed for %s %s content for duplicates keys %s",
                              content.meta_info.core_or_ext_type.value, content.meta_info.type, content.keys)
                    validation_content_success = False

                if not self._validate_columns(class_type, content, error_df):
                    log.error("Validation failed for %s %s content for duplicate columns",
                              content.meta_info.core_or_ext_type.value, content.meta_info.type)
                    validation_content_success = False

                if validation_content_success:
                    log.info("Validation successful for %s %s content for unique keys %s",
                             content.meta_info.core_or_ext_type.value, content.meta_info.type, content.keys)
                else:
                    validation_success = False

        return True if validation_success else False

    def extract_csv_content(self, csv_info: ContentData, core_ext_type: CoreOrExtType, extra_read_param: dict = None):
        """Read the data from a CSV description into a content frame and include it in the Dwca.

        :param csv_info: The CSV file(s)
        :param core_ext_type: Whether this is a core or extension content frame
        :param extra_read_param: extra read param to use when reading csv
        """
        if isinstance(csv_info.data, pd.DataFrame) :
            csv_content = csv_info.data
        elif isinstance(csv_info.data, io.TextIOWrapper):
            csv_content = self._read_csv(csv_file=csv_info.data, extra_param=extra_read_param)
        else:
            csv_content = self._combine_contents(contents=csv_info.data, csv_encoding=csv_info.csv_encoding,
                                                 extra_read_param=extra_read_param)

        # Use default keys if not provided
        if core_ext_type == CoreOrExtType.CORE:
            override_keys = {csv_info.type: csv_info.keys} if csv_info.keys and len(csv_info.keys) > 0 else None
            keys = get_keys(class_type=csv_info.type, override_content_keys=override_keys)
        else:
            keys = self.core_content.keys
        core_id_field: str = ""
        if len(keys) > 1:
            if core_ext_type == CoreOrExtType.CORE:
                core_id_field = self._update_core_ids(csv_content)
                self._build_index_for_content(csv_content, keys)
            elif core_ext_type == CoreOrExtType.EXTENSION:
                csv_content, core_id_field = self._update_extension_ids(
                    csv_content, self.core_content.df_content, keys)
        elif len(keys) > 0:
            core_id_field = keys[0]

        if csv_info.associated_files_loc:
            self._update_associated_files([csv_info.associated_files_loc])

        meta_element_info = MetaElementInfo(
            core_or_ext_type=core_ext_type, type=csv_info.type,
            csv_encoding=self.defaults_prop.csv_encoding, ignore_header_lines='1')
        content = DfContent(df_content=csv_content, meta_info=meta_element_info)
        self._update_meta_fields(content, core_id_field)

        if core_ext_type == CoreOrExtType.CORE:
            content.keys = keys
            self.core_content = content
        else:
            content.keys = csv_info.keys
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
            lineterminator='\r\n' if meta_info.csv_encoding.csv_eol == '\\r\\n' else meta_info.csv_encoding.csv_eol,
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

    def write_dwca(self, output_dwca: Union[str, BytesIO]):
        """Write a full DwCA to a zip file
        Any parent directories needed are created during writing.

        :param output_dwca: The file path to write the .zip file to or dwca in memory
        """
        # Make sure that the memory that's being overwritten is r
        if isinstance(output_dwca, BytesIO):
            output_dwca.flush()
            output_dwca.truncate(0)
            output_dwca.seek(0)
        with ZipFile(output_dwca, 'w', allowZip64=True,
                     compression=zipfile.ZIP_DEFLATED) as dwca_zip:
            self._write_df_content_to_zip_file(dwca_zip=dwca_zip, content=self.core_content)
            for ext in self.ext_content:
                self._write_df_content_to_zip_file(dwca_zip=dwca_zip, content=ext)
            dwca_zip.writestr(self.defaults_prop.meta_xml_filename, str(self.meta_content))
            if self.eml_content:
                dwca_zip.writestr(self.defaults_prop.eml_xml_filename, self.eml_content)
            self._write_associated_files(dwca_zip=dwca_zip)
            log.info("Dwca zip file created in %s: size %i, containing files: %s",
                     output_dwca, dwca_zip.start_dir, ",".join(dwca_zip.namelist()))
            dwca_zip.close()

    def _read_csv(self,
                  csv_file: Union[str, io.TextIOWrapper],
                  csv_encoding_param: CSVEncoding = MISSING,
                  columns: list = None,
                  ignore_header_lines: int = 0,
                  iterator: bool = False,
                  chunksize: int = 100,
                  nrows: int = 0,
                  extra_param: dict = None) -> Union[pd.DataFrame, parsers.TextFileReader]:
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
        :param extra_param: A dictionary containing extra values to use.
        :return: Either a data frame or a reader, depending on the iterator parameter
        """
        if extra_param is None:
            extra_param = {}
        if csv_encoding_param is MISSING:
            csv_encoding_param = self.defaults_prop.csv_encoding

        # Note: having lineterminator as \n leaves \r in the column text if \r\n is present.
        #       pandas read_csv cannot support passing in \r\n, omitting lineterminator seem to
        #       work properly passing in escapechar as double-quote into pandas does not
        #       work with csv that have double quotes around every field, only set escapechar,
        #       if it is other than double-quotes.
        escape_char = csv_encoding_param.csv_escape_char if csv_encoding_param.csv_escape_char != '"' else None
        quote_char = csv_encoding_param.csv_text_enclosure if csv_encoding_param.csv_text_enclosure != '' else '"'
        line_terminator = csv_encoding_param.csv_eol \
            if (csv_encoding_param.csv_eol not in ['\r\n', '\n', '\\r\\n']) \
            else None

        try:
            read_param = {"delimiter": csv_encoding_param.csv_delimiter,
                          "escapechar": escape_char,
                          "quotechar": quote_char,
                          "lineterminator": line_terminator,
                          "names": columns,
                          "skiprows": ignore_header_lines,
                          "skip_blank_lines": True,
                          "dtype": 'str',
                          "index_col": False,
                          "chunksize": chunksize if iterator else None,
                          "iterator": iterator,
                          "nrows": nrows if nrows > 0 else None}
            if extra_param and len(extra_param) > 0:
                def __is_integer(s: str) -> bool:
                    if re.match(r'^[+-]?[0-9]+$', s):
                        return True
                    return False

                def __parse_bool(value_bool: str):
                    if isinstance(value_bool, bool):
                        return value_bool

                    value_bool = str(value_bool).strip().lower()
                    if value_bool in ('true', 'yes', '1'):
                        return True
                    elif value_bool in ('false', 'no', '0'):
                        return False
                    return None

                for key, value in extra_param.items():
                    if value.lower() in ["true", "false"]:
                        read_param.update({key: __parse_bool(value)})
                    elif value.lower() in ["nan", "none"]:
                        read_param.update({key: None})
                    elif __is_integer(value):
                        read_param.update({key: int(value)})
                    else:
                        read_param.update({key: value})

            ret_val = pd.read_csv(csv_file, **read_param)

            if isinstance(ret_val, pd.DataFrame):
                # Drop rows where all the columns are Nan
                ret_val.dropna(how="all", inplace=True)
                log.debug("Extracted %d rows from csv %s", len(ret_val), csv_file)

                # Strip column header spaces
                ret_val.rename(str.strip, axis='columns', inplace=True)

            return ret_val

        except EmptyDataError:
            if columns:
                log.error(f"The file may be empty {csv_file}")
            else:
                log.error(f"The expected columns: %s are not present in the {csv_file}. "
                          f"The file may be empty", ','.join(columns))

            return pd.DataFrame()
