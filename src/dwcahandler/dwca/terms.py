import pandas as pd
from dataclasses import dataclass, field, asdict
import os
from pathlib import Path
import re

this_dir, this_filename = os.path.split(__file__)

def absolute_file_paths(directory):
    """Convert files in a directory into absolute paths and return
    as a generator

    :param directory: The directory to scan.
    :return: An absolute file path.
    """
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            if re.fullmatch(r'.+\..*', f):
                yield os.path.abspath(os.path.join(dirpath, f))


@dataclass
class Terms:
    TERMS_DWC_URL = "https://raw.githubusercontent.com/tdwg/rs.tdwg.org/master/terms/terms.csv"
    DWC_FILENAME = 'darwin-core-terms.csv'
    DUBLIN_CORE_FILENAME = 'dublin-core-terms.csv'
    TERMS_DIR = f"{this_dir}/terms"
    DWC_FILE_PATH = f"{TERMS_DIR}/{DWC_FILENAME}"
    DUBLIN_CORE_PATH = f"{TERMS_DIR}/{DUBLIN_CORE_FILENAME}"

    terms_path: list[Path] = field(default_factory=lambda: [c for c in absolute_file_paths(Terms.TERMS_DIR)],
                                   init=False)
    terms_df: pd.DataFrame = field(default_factory=pd.DataFrame, init=False)
    dwc_terms_df: pd.DataFrame = field(default_factory=pd.DataFrame, init=False)

    def __post_init__(self):
        def _add_to_df(existing_df: pd.DataFrame, df: pd.DataFrame):
            if not existing_df.empty:
                return existing_df.merge(df, how='outer', left_on=['term', 'uri'], right_on=['term', 'uri'])
            return df

        for term_path in self.terms_path:
            df = pd.read_csv(term_path, dtype='str')
            self.terms_df = _add_to_df(self.terms_df, df)
            if term_path == Terms.DWC_FILE_PATH or term_path == Terms.DUBLIN_CORE_PATH:
                self.dwc_terms_df = _add_to_df(self.dwc_terms_df, df)

    @staticmethod
    def update_dwc_terms():
        """
        Pull the latest terms from gbif dwc csv url and update the darwin core vocab terms in the package
        This is still WIP, do we to pull the
        :return:
        """
        df = pd.read_csv(Terms.TERMS_DWC_URL, delimiter=",", encoding='utf-8', dtype='str')
        df = df[df["term_deprecated"].isnull()]
        dwc_df = pd.DataFrame()
        dwc_df['term'] = df['term_localName']
        dwc_df['uri'] = df['term_isDefinedBy'] + df['term_localName']
        dwc_df.to_csv(Terms.DWC_FILE_PATH, index=False)
        return dwc_df
