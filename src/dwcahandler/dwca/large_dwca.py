"""
Large DwCA Support
------------------

Large DwCAs are extensions of the Dwca class that use chunking and a work directory to
handle large datasets.
"""
from pathlib import Path
import pandas as pd
from dwcahandler.dwca import Dwca, DfContent, Stat
from dwcahandler.dwca import CSVEncoding, MetaElementInfo
import uuid
from dataclasses import dataclass, field, MISSING
from zipfile import ZipFile
from functools import wraps
import tempfile
import logging

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
log = logging.getLogger("LargeDwca")


@dataclass
class LargeDfContent:
    """Data frame content in the form of a list of files"""
    meta_info: MetaElementInfo
    # store list of pickle files
    df_content: list[Path] = field(default_factory=list)
    keys: list[str] = field(init=False, default_factory=list)
    stat: Stat = Stat(0)


@dataclass
class LargeDwca(Dwca):
    """A large DwCA based on a lists of chunked files.

    Individual content tables, instead of being simple data frames are split and placed into temporary files.
    Processes are then applied across the files in sequence.

    This class essentially works by wrapping the Dwca functions so that they are applied
    across the lists of chunked files.
    See the Dwca class for a description of methods.
    """
    core_content: LargeDfContent = field(init=False)
    ext_content: list[LargeDfContent] = field(init=False, default_factory=list)
    CHUNK_SIZE: int = 1000
    temp_folder: str = './dwca/output/pickle'

    def __post_init__(self):
        Path(self.temp_folder).mkdir(parents=True, exist_ok=True)
        log.info("Using work folder %s", self.temp_folder)
        super().__post_init__()

    def update_content(in_place=True):
        """A wrapper that updates a series of files in content"""
        def unpack_pickle_file(function):
            @wraps(function)
            def update_wrapper(self, content, *args):
                for file in content:
                    df_temp = self._get_df(file)
                    ret = function(self, df_temp, *args)
                    if in_place:
                        self._save_df(df_temp, file)
                    else:
                        self._save_df(ret, file)
                return content

            return update_wrapper

        return unpack_pickle_file

    def update_content_from_lookup_content(update_content=True, in_place=False):
        """A wrapper that updates a series of files from a lookup"""
        def unpack_pickle_files(function):
            # update pickle file of content based on the another content
            @wraps(function)
            def unpack_pickle_files_wrapper(self, content, another_content, *args, **kwargs):
                remove_list = []
                return_val_info = pd.DataFrame()
                for file in content:
                    df = self._get_df(file)
                    for file2 in another_content:
                        df2 = self._get_df(file2)
                        return_value = function(self, df, df2, *args, **kwargs)
                        if update_content:
                            if in_place:
                                self._save_df(df, file)
                                self._save_df(df2, file2)
                            else:
                                if isinstance(return_value, pd.DataFrame):
                                    if return_value.empty:
                                        remove_list.append(file)
                                    else:
                                        self._save_df(return_value, file)
                        else:
                            if isinstance(return_value, pd.DataFrame):
                                return_val_info = pd.concat([return_val_info, return_value])

                if len(remove_list) > 0:
                    content.remove(remove_list)
                    return content

                return return_value if update_content else return_val_info

            return unpack_pickle_files_wrapper

        return unpack_pickle_files

    @update_content(in_place=True)
    def _update_core_ids(self, core_df):
        super()._update_core_ids(core_df)

    @update_content(in_place=False)
    def _update_extension_ids(self, csv_content, core_df_content, link_col):
        # Having link_col as index and column raises ambiguous error in merge
        if set(link_col).issubset(set(csv_content.columns.to_list())) and set(link_col).issubset(
                set(csv_content.index.names)):
            csv_content.reset_index(inplace=True, drop=True)

        merged_df = pd.DataFrame()
        for file in core_df_content:
            df_lookup = self._get_df(file)
            merged_df = pd.concat([merged_df,
                                   csv_content.merge(df_lookup.loc[:, 'id'], left_on=link_col, right_on=link_col,
                                                     how='inner')])
            if (len(csv_content) == len(merged_df) and ('id' in merged_df) and (
                    len(merged_df[merged_df['id'].isnull()]) == 0)):
                break

        csv_content = merged_df
        if 'id' in csv_content.columns.to_list():
            if 'coreid' in csv_content:
                csv_content.pop('coreid')
            col = csv_content.pop('id')
            csv_content.insert(0, col.name, col)
            csv_content.rename(columns={"id": "coreid"}, inplace=True)

        return csv_content

    @update_content(in_place=True)
    def _build_index_for_content(self, content, keys):
        super()._build_index_for_content(content, keys)

    def _combine_contents(self, contents: list, csv_encoding, use_chunking: bool = True):
        if isinstance(contents[0], pd.DataFrame):
            file_name = self._save_df(contents[0])
            return [file_name]
        else:
            return super()._combine_contents(contents, csv_encoding, use_chunking)

    def _init_content(self):
        return []

    def _set_content(self, csv_content, meta_element_info):
        return LargeDfContent(df_content=csv_content, meta_info=meta_element_info,
                              stat=Stat(self.count_stat(csv_content)))

    def _read_header(self, df_content) -> list[str]:
        if isinstance(df_content, pd.DataFrame):
            return super()._read_header(df_content)
        elif (len(df_content) > 0):
            df = self._get_df(df_content[0])
            return super()._read_header(df)
        return []

    @update_content(in_place=True)
    def _add_content_index(self, content, indexes, is_existing_column, drop_existing_index):
        if drop_existing_index:
            content.reset_index(drop=True, inplace=True)

        if not is_existing_column:
            content.set_index(indexes, inplace=True, drop=True)
        else:
            content.set_index(indexes, inplace=True, drop=False)

    def _extract_media(self, content, assoc_media_col: str):
        image_df = pd.DataFrame()
        for file in content:
            df = self._get_df(file)
            image_df = pd.concat([image_df, super()._extract_media(df, assoc_media_col)])
            self._save_df(df, file)
        return image_df

    def _extract_keys(self, core_content, keys):
        keys_df = pd.DataFrame()
        for content in core_content:
            df = self._get_df(content)
            keys_df = pd.concat([keys_df, df[keys]])
        return keys_df

    def _extract_core_keys(self, core_content, keys):
        core_keys_list = []
        for content in core_content:
            if 'core_keys' not in locals():
                core_keys = pd.DataFrame()
            df = self._get_df(content)
            core_keys = pd.concat([core_keys, super()._extract_core_keys(df, keys)])
            if len(core_keys) > self.CHUNK_SIZE * 4:
                file_name = self._save_df(core_keys)
                core_keys_list.append(file_name)
                del core_keys

        if 'core_keys' in locals() and not core_keys.empty:
            file_name = self._save_df(core_keys)
            core_keys_list.append(file_name)

        return core_keys_list

    @update_content(in_place=True)
    def _update_column(self, df, col, other_col, stat):
        super()._update_column(df, col, other_col, stat)

    @update_content(in_place=True)
    def _add_ext_lookup_key(self, df_content, core_df_content, core_keys, keys):
        super()._add_ext_lookup_key(df_content, core_df_content, core_keys, keys)

    def _cleanup_keys(self, core_index_keys):
        if isinstance(core_index_keys, list):
            for pickle_file in core_index_keys:
                pickle_file.unlink(True)
        else:
            super()._cleanup_keys(core_index_keys)

    def _add_core_key(self, df_content, core_df_content, core_keys):
        for content in core_df_content:
            core_df = self._get_df(content)
            super()._add_core_key(df_content, core_df, core_keys)
            if (set(core_keys).issubset(set(df_content.columns.to_list())) and (
            df_content[df_content[core_keys].isnull()].empty)):
                break

    @update_content_from_lookup_content(update_content=True, in_place=True)
    def _add_col_and_update_values(self, content, delta_content, keys, update, stat):
        return super()._add_col_and_update_values(content, delta_content, keys, update, stat)

    @update_content_from_lookup_content(update_content=False)
    def _find_records_exist_in_both(self, core_content, delta_core_content):
        return super()._find_records_exist_in_both(core_content, delta_core_content)

    @update_content_from_lookup_content(update_content=True, in_place=True)
    def _delete_old_ext_records(self, content, core_content, delta_core_content, core_keys):
        super()._delete_old_ext_records(content, core_content, delta_core_content, core_keys)

    def count_stat(self, contents):
        count = 0
        for content in contents:
            df = self._get_df(content)
            count += len(df)
        return count

    def _filter_content(self, df_content, delta_df_content):
        new_rows = []
        for file in delta_df_content:
            df = self._get_df(file)
            for file2 in df_content:
                df2 = self._get_df(file2)
                df = super()._filter_content(df2, df)
                if df.empty:
                    break

            if not df.empty:
                file_name = self._save_df(df)
                new_rows.append(file_name)

        return new_rows

    def __create_df(self, df_content):
        new_df = pd.DataFrame()
        if len(df_content) > 0:
            column_list = self._get_df(df_content[0]).columns.tolist()
            new_df = pd.DataFrame(columns=column_list)
        return new_df

    def _add_new_rows(self, df_content, new_rows):
        new_df = self.__create_df(df_content)
        for a_file in new_rows:
            new_df = pd.concat([new_df, self._get_df(a_file)])
            if len(new_df) >= self.CHUNK_SIZE:
                df_content.append(self._save_df(new_df))
                new_df = self.__create_df(df_content)

        if len(new_df) >= 0:
            df_content.append(self._save_df(new_df))

        return df_content

    def _read_csv(self, csv_file: str, csv_encoding_param: CSVEncoding = MISSING, columns: list = [],
                  ignore_header_lines: int = 0,
                  iterator: bool = False, chunksize: int = 100, nrows: int = 0) -> list:
        pickle_list: list[str] = []
        df_reader = super()._read_csv(csv_file, columns=columns, csv_encoding_param=csv_encoding_param,
                                      ignore_header_lines=ignore_header_lines, iterator=True, chunksize=self.CHUNK_SIZE,
                                      nrows=nrows)
        for df in df_reader:
            df.dropna(how="all", inplace=True)
            file_name = self._save_df(df)
            pickle_list.append(file_name)

        log.info("Extracted csv_file into %d pickle files", len(pickle_list))
        return pickle_list

    def _get_df(self, content):
        df_temp = pd.DataFrame()
        with open(str(content), 'rb') as pfile:
            df_temp = pd.read_pickle(pfile, compression={'method': 'gzip'})
            pfile.close()
        return df_temp

    def _save_df(self, df, file=''):
        if not file:
            file = Path(f'{self.temp_folder}/{uuid.uuid4()}.pkl.gz')
        df.to_pickle(file, compression={'method': 'gzip', 'compresslevel': 1, 'mtime': 1})
        return file

    def _write_df_content_to_zip_file(self, dwca_zip: ZipFile, content: DfContent):
        with tempfile.NamedTemporaryFile(mode='a', dir=self.temp_folder, delete=False) as tempFile:
            for idx, file in enumerate(content.df_content):
                df = self._get_df(file)
                header = True if (content.meta_info.ignore_header_lines == '1' and idx == 0) else False
                try:
                    tempFile.write(self._to_csv(df, content.meta_info, header))
                except Exception as error:
                    log.error("Error in writing csv: %s, %d. Error %s", file, idx, error)
            tempFile.close()

        dwca_zip.write(tempFile.name, content.meta_info.file_name)
        Path(tempFile.name).unlink(True)

    def __del__(self):
        def clean_df_content(df_content):
            for pickle_file in df_content:
                pickle_file.unlink(True)

        clean_df_content(self.core_content.df_content)
        for ext_content in self.ext_content:
            clean_df_content(ext_content.df_content)
