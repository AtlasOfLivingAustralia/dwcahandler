import os
from pathlib import Path
from dataclasses import dataclass, field
import re
import pandas as pd
import logging as log
from urllib.parse import urlparse
from urllib.request import urlopen
from enum import Enum
from typing import NamedTuple
import requests

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
                yield os.path.abspath(str(os.path.join(dirpath, f)))


class NsPrefix(Enum):
    """
    Enumeration of class or terms prefix
    """
    DWC = "dwc"
    DC = "dc"
    GBIF = "gbif"
    OBIS = "obis"


class ExtInfo(NamedTuple):
    """
    Extension info
    """
    uri: str
    prefix: NsPrefix
    namespace: str


class GbifRegisteredExt(ExtInfo, Enum):
    """
    Supported Gbif extensions. Add more extensions to expand the class row type and terms
    """
    EXTENDED_MEASUREMENT_OR_FACT = ExtInfo(uri="http://rs.iobis.org/obis/terms/ExtendedMeasurementOrFact",
                                           prefix=NsPrefix.OBIS,
                                           namespace="http://rs.iobis.org/obis/terms/")
    SIMPLE_MULTIMEDIA = ExtInfo(uri="http://rs.gbif.org/terms/1.0/Multimedia",
                                prefix=NsPrefix.GBIF,
                                namespace="http://rs.gbif.org/terms/1.0/")


@dataclass
class Terms:
    """
    Terms class to manage the terms and class row types used in the dwca
    """

    GBIF_EXT = "https://rs.gbif.org/extensions.json"

    GBIF_REGISTERED_EXTENSION = [e for e in GbifRegisteredExt]

    DWC_SOURCE_URL = "https://raw.githubusercontent.com/tdwg/rs.tdwg.org/master/terms/terms.csv"

    TERMS_FILENAME = "terms.csv"
    CLASS_ROW_TYPE = "class-rowtype.csv"

    TERMS_DIR = os.path.join(this_dir, "terms")
    TERMS_FILE_PATH = os.path.join(TERMS_DIR, TERMS_FILENAME)
    CLASS_ROW_TYPE_PATH = os.path.join(TERMS_DIR, CLASS_ROW_TYPE)

    terms_df: pd.DataFrame = field(default_factory=pd.DataFrame, init=False)
    class_df: pd.DataFrame = field(default_factory=pd.DataFrame, init=False)

    def __post_init__(self):
        self.terms_df = pd.read_csv(Terms.TERMS_FILE_PATH, dtype='str')
        self.class_df = pd.read_csv(Terms.CLASS_ROW_TYPE_PATH, dtype='str')

    @staticmethod
    def _update_class_csv(ns: NsPrefix, updates: pd.DataFrame):
        """
        Update class rowtype by replacing all the rows by prefix.

        :param ns: Name prefix
        :param updates: dataframe containing the class rows to update
        """
        if len(updates) > 0 and "class_uri" in updates.columns.tolist():
            updates.insert(0, "class",
                           updates["class_uri"].apply(
                               lambda x: f"{Terms.extract_term(term_string = x, add_underscore = True).upper()}"))
            updates["prefix"] = ns.value

            Terms._update_csv(ns, updates, True)
            return updates

    @staticmethod
    def _update_csv(ns: NsPrefix, updates: pd.DataFrame, is_class: bool = True):
        """
        Update class rowtype or terms by replacing all the rows by prefix.

        :param ns: Name prefix
        :param updates: dataframe containing the class rows or terms to update
        :param is_class: True if it is a class rowtype. False if this is terms
        """

        col_list = ["prefix", "class", "class_uri"] if is_class else ["prefix", "term", "uri"]
        file = Terms.CLASS_ROW_TYPE_PATH if is_class else Terms.TERMS_FILE_PATH

        if all(col in updates.columns.tolist() for col in col_list):
            df = updates
            if Path(file).is_file():
                df = pd.read_csv(file)
                if len(df) > 0:
                    df = df[df["prefix"] != ns.value]
                    df = pd.concat([df, updates[col_list]], ignore_index=False)

            df.to_csv(file, index=False)
            log.info("Rows updated in %s: %s of %s",
                     Path(Terms.CLASS_ROW_TYPE).name, len(updates), len(df))
        else:
            log.info("No updates to class csv %s", Path(Terms.CLASS_ROW_TYPE).name)

    @staticmethod
    def get_dwc_source_data() -> pd.DataFrame:
        return pd.read_csv(Terms.DWC_SOURCE_URL, delimiter=",", encoding='utf-8', dtype='str')

    @staticmethod
    def update_dwc_terms():
        """
        Pull the latest terms from gbif dwc csv url and update the darwin core vocab terms in the package
        For reference: dublin-core-terms is derived from
                        https://www.dublincore.org/specifications/dublin-core/dcmi-terms/ terms namespace
        :return: dwc_df dataframe containing the updated dwc term
                 dwc_class_df dataframe containing the updated dwc class
        """
        df = Terms.get_dwc_source_data()
        df = df[df["term_deprecated"].isnull()]
        dwc_df = pd.DataFrame()
        dwc_df['term'] = df['term_localName']
        dwc_df['uri'] = df['term_isDefinedBy'] + df['term_localName']
        dwc_df["prefix"] = NsPrefix.DWC.value

        if len(dwc_df) > 0:
            Terms._update_csv(NsPrefix.DWC, dwc_df, False)

        dwc_class_df = pd.DataFrame()
        dwc_class_df["class_uri"] = df["tdwgutility_organizedInClass"].unique()
        dwc_class_df = dwc_class_df[dwc_class_df["class_uri"].notna()]

        log.info("Total terms downloaded: %i", len(dwc_df))
        log.info("Total class downloaded: %i", len(dwc_class_df))

        if len(dwc_class_df) > 0:
            dwc_class_df = Terms._update_class_csv(NsPrefix.DWC, dwc_class_df)

        return dwc_df, dwc_class_df

    @staticmethod
    def extract_term(term_string, add_underscore: bool = False):
        """
        Find a term name based on a term or a URI

        :param term_string: The term or URI
        :param add_underscore: if true, adds _ to before capital letter before a camel case.
                                for eg: occurrenceID to occurrence_ID
        :return: The term name
        """
        path_entity = urlparse(term_string)
        path_str = path_entity.path
        match = re.search(r'/([^/]*)$', path_str)
        if match is not None:
            term = match[1]
            word = re.sub(pattern="(?!^)([A-Z])", repl=r"_\1", string=term) if add_underscore else term
            return word

        return term_string

    @staticmethod
    def get_class_row_types():
        """
        This is called by the meta class row type to build the enumeration list
        """
        class_df = pd.read_csv(Terms.CLASS_ROW_TYPE_PATH)
        class_list = list(tuple(zip(class_df["class"], class_df["class_uri"])))
        return class_list

    @staticmethod
    def update_gbif_ext():
        """
        Update the class row type and terms specified by GBIF_REGISTERED_EXTENSION and update by prefix
        """
        def _get_latest(identifier: str):
            d = requests.get(Terms.GBIF_EXT).json()
            gbif_ext_df = pd.DataFrame.from_dict(d["extensions"])
            ext_df = gbif_ext_df[(gbif_ext_df["identifier"] == identifier) & (gbif_ext_df["isLatest"])]
            url: str = ""
            if len(ext_df) > 0 and "url" in ext_df.columns.tolist():
                url = ext_df["url"].values[0]
            return url

        def _extract_term_info(every_term: tuple) -> list:
            def _extract_value(text: str):
                return text.replace('\\', "").\
                            replace('"', "").replace("'", "").split("=")[1]

            term_name = _extract_value(every_term[0])
            namespace = _extract_value(every_term[1])
            uri = _extract_value(every_term[2])

            return [term_name, namespace, uri]

        for supported_ext in Terms.GBIF_REGISTERED_EXTENSION:
            url = _get_latest(supported_ext.uri)
            if url:
                update_class = pd.DataFrame([supported_ext.uri], columns=["class_uri"])
                Terms._update_class_csv(supported_ext.prefix, update_class)

                with urlopen(url) as f:

                    xml_str = str(f.read())
                    reg_exp = r'<property.*?(name=.*?)\s+.*?(namespace=.*?)\s+.*?(qualName=.*?)\s+.*?/>'
                    list_of_ns_terms = re.findall(reg_exp, xml_str)
                    log.info("List of terms found for %s: %d", url, len(list_of_ns_terms))

                    term_info = []
                    for every_term in list_of_ns_terms:
                        term_info.append(_extract_term_info(every_term))

                    df = pd.DataFrame(term_info, columns=["term", "namespace", 'uri'])
                    std_ns = ["http://rs.tdwg.org/dwc/terms/", "http://purl.org/dc/terms/"]
                    existing_terms = Terms().terms_df
                    extra_terms_df = df[(df["namespace"].isin(std_ns)) & (~df["uri"].isin(existing_terms["uri"]))]
                    log.info("Additional standard terms found:\n%s", extra_terms_df)
                    new_terms = df[~df["uri"].isin(existing_terms["uri"])]
                    if len(new_terms) > 0:
                        new_terms["prefix"] = supported_ext.prefix.value
                        Terms._update_csv(supported_ext.prefix, new_terms, False)

    @staticmethod
    def update_terms():
        Terms.update_dwc_terms()
        Terms.update_gbif_ext()
