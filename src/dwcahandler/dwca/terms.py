import os
from dataclasses import dataclass, field
import re
import pandas as pd
import logging as log
from urllib.parse import urlparse
from urllib.request import urlopen
import numpy as np
from enum import Enum
import requests

this_dir, this_filename = os.path.split(__file__)

log.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=log.DEBUG)
log = log.getLogger("DwcaTerms")

TERMS_DIR = os.path.join(this_dir, "terms")
REGISTER_FILENAME = "extension-register.csv"
EXTENSION_REGISTER_PATH = os.path.join(TERMS_DIR, REGISTER_FILENAME)


def get_ns_prefix():
    """
    This is called by the prefix enumeration list
    """
    df = pd.read_csv(EXTENSION_REGISTER_PATH)
    df = pd.DataFrame(df["prefix"], columns=["prefix"])
    df = df.drop_duplicates()
    df.loc[df.index.max() + 1] = "dc"  # Add dc prefix
    ns_list = list(tuple(zip(df["prefix"].str.upper(), df["prefix"])))
    return ns_list


NsPrefix = Enum("NsPrefix", dict(get_ns_prefix()))


@dataclass
class Terms:
    """
    Terms class to manage the terms and class row types used in the dwca
    """

    GBIF_EXT = "https://rs.gbif.org/extensions.json"

    GBIF_REGISTERED_EXTENSION = pd.DataFrame()

    DWC_SOURCE_URL = "https://raw.githubusercontent.com/tdwg/rs.tdwg.org/master/terms/terms.csv"

    TERMS_FILENAME = "terms.csv"
    CLASS_ROW_TYPE = "class-rowtype.csv"

    TERMS_FILE_PATH = os.path.join(TERMS_DIR, TERMS_FILENAME)
    CLASS_ROW_TYPE_PATH = os.path.join(TERMS_DIR, CLASS_ROW_TYPE)

    terms_df: pd.DataFrame = field(default_factory=pd.DataFrame, init=False)
    class_df: pd.DataFrame = field(default_factory=pd.DataFrame, init=False)

    def __post_init__(self):
        self.terms_df = pd.read_csv(Terms.TERMS_FILE_PATH, dtype='str')
        self.class_df = pd.read_csv(Terms.CLASS_ROW_TYPE_PATH, dtype='str')

    def _update_class_df(self, ns, updates: pd.DataFrame):
        """
        Only updates class rowtypes if it's not in the current class rowtype.
        This is to ensure the existing enum names stays intact (used by MetaElementType enums)

        :param ns: Name prefix
        :param updates: dataframe containing the class rows to update
        """
        def __get_class_term(existing_class_df, class_uri, prefix):
            class_term = Terms.extract_term(term_string=class_uri, add_underscore=True).upper()
            if len(existing_class_df[existing_class_df['class'].str.contains(class_term)]) > 0:
                return f"{prefix.upper()}_{class_term}"
            return class_term

        if len(updates) > 0 and "class_uri" in updates.columns.tolist():
            updates = updates[(~updates["class_uri"].isin(self.class_df["class_uri"]))]

            if len(updates) > 0:
                updates.insert(0, "class",
                               updates["class_uri"].apply(
                                   lambda x: f"{__get_class_term(self.class_df, x, ns.value)}"))
                updates["prefix"] = ns.value
                return self._update_df(ns, updates, self.class_df)

        return self.class_df

    def _update_df(self, ns, updates: pd.DataFrame, df: pd.DataFrame):
        """
        Update class row type or terms by replacing all the rows by prefix.

        :param ns: Name prefix
        :param updates: dataframe containing the class rows or terms to update
        :param df: dataframe to update
        """
        def __get_update_info(update_df: pd.DataFrame):
            update_type: str = "term"
            count = len(update_df)
            if 'class' in update_df.columns.tolist():
                update_type = "class"
            return count, update_type

        col_list = df.columns.tolist()

        if all(col in updates.columns.tolist() for col in col_list):
            df = pd.concat([df, updates[col_list]], ignore_index=True)
            log.info("Refreshed %s %s prefix %s", *__get_update_info(updates), ns)
        return df

    @staticmethod
    def get_dwc_source_data() -> pd.DataFrame:
        return pd.read_csv(Terms.DWC_SOURCE_URL, delimiter=",", encoding='utf-8', dtype='str')

    def update_dwc_terms(self):
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
            self.terms_df = self._update_df(NsPrefix.DWC, dwc_df, self.terms_df)

        dwc_class_df = pd.DataFrame()
        dwc_class_df["class_uri"] = df["tdwgutility_organizedInClass"].unique()
        dwc_class_df = dwc_class_df[dwc_class_df["class_uri"].notna()]

        log.info("Total terms downloaded: %i", len(dwc_df))
        log.info("Total class downloaded: %i", len(dwc_class_df))

        if len(dwc_class_df) > 0:
            self.class_df = self._update_class_df(NsPrefix.DWC, dwc_class_df)

        return self.terms_df, self.class_df

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
        if path_str:
            fragment = path_entity.fragment
            match = re.search(r'/([^/]*)$', path_str)
            if match is not None:
                term = match[1]
                word = re.sub(pattern="(?!^)(?<![DNA])([A-Z])", repl=r"_\1", string=term) if add_underscore else term
                return f"{word}_{fragment}" if fragment else word
        else:
            raise ValueError("Error reading from meta xml. This is caused by empty term in the meta xml")

        return term_string

    @staticmethod
    def get_class_row_types():
        """
        This is called by the meta class row type to build the enumeration list
        """
        class_df = pd.read_csv(Terms.CLASS_ROW_TYPE_PATH)
        class_list = list(tuple(zip(class_df["class"], class_df["class_uri"])))
        return class_list

    def update_gbif_ext(self):
        """
        Update the class row type and terms specified by GBIF_REGISTERED_EXTENSION and update by prefix
        """
        def _extract_term_info(current_term: tuple) -> list:
            def _extract_value(text: str):
                return text.replace("\\n", "").replace('\\', ""). \
                            replace('"', "").replace("'", "").split("=")[1]

            term_name = _extract_value(current_term[0])
            namespace = _extract_value(current_term[1])
            uri = _extract_value(current_term[2])

            return [term_name, namespace, uri]

        def _get_ns_prefix(val: str):
            ns_prefix = [p for p in NsPrefix if p.value == val]
            if len(ns_prefix) > 0:
                return ns_prefix[0]
            else:
                return None

        gbif_registered_ext = pd.read_csv(EXTENSION_REGISTER_PATH)
        # gbif_registered_ext = pd.DataFrame(data={'url': ["http://rs.gbif.org/extension/gbif/1.0/dna_derived_data_2024-07-11.xml"],
        #                                         "identifier": ["http://rs.gbif.org/terms/1.0/DNADerivedData"],
        #                                         "prefix": ["gbif"]})

        for index, supported_ext in gbif_registered_ext.iterrows():
            url = supported_ext["url"]
            prefix = _get_ns_prefix(supported_ext["prefix"])
            if url:
                update_class = pd.DataFrame([supported_ext["identifier"]], columns=["class_uri"])
                self.class_df = self._update_class_df(prefix, update_class)

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
                    existing_terms = self.terms_df
                    extra_terms_df = df[(df["namespace"].isin(std_ns)) & (~df["uri"].isin(existing_terms["uri"]))]
                    if len(extra_terms_df) > 0:
                        log.info("Additional standard terms found:\n%s", extra_terms_df)
                    new_terms = df[~df["uri"].isin(existing_terms["uri"])].copy()
                    if len(new_terms) > 0:
                        new_terms.loc[:, "prefix"] = prefix.value
                        self.terms_df = self._update_df(prefix, new_terms, self.terms_df)

        return self.terms_df, self.class_df

    @staticmethod
    def update_terms():
        """
        Refresh all the terms except for dublin core terms with dc prefix. As these are not obtained dynamically
        :return:
        """
        def __sort_values(df_to_sort: pd.DataFrame, sorting_column: str) -> pd.DataFrame:
            """
            Make sure dc and dwc prefixes stay on top
            :param df_to_sort: dataframe to be sorted
            :param sorting_column: other column to sort
            :return: sorted dataFrame
            """
            df_to_sort = df_to_sort.sort_values(by=["prefix", sorting_column], key=lambda x: x.str.lower())
            std_filter_df = df_to_sort.prefix.isin(["dc", "dwc"])
            std_df = df_to_sort[std_filter_df].copy()
            ext_df = df_to_sort[~std_filter_df].copy()
            return pd.concat([std_df, ext_df], ignore_index=True)

        log.info("Current class and terms")

        terms = Terms()
        exclude_update_prefixes = [NsPrefix.DC.value]
        print("Here is what we have before update: ")
        print(terms.class_df.groupby(["prefix"]).agg(
            class_prefix_count=pd.NamedAgg(column="prefix", aggfunc="count")
        ))
        print(terms.terms_df.groupby(["prefix"]).agg(
            term_prefix_count=pd.NamedAgg(column="prefix", aggfunc="count")
        ))
        terms.terms_df = terms.terms_df[terms.terms_df.prefix.isin(exclude_update_prefixes)]
        terms.update_dwc_terms()
        terms.update_gbif_ext()
        terms.class_df = __sort_values(terms.class_df, "class")
        terms.terms_df = __sort_values(terms.terms_df, "term")
        terms.class_df.to_csv(Terms.CLASS_ROW_TYPE_PATH, index=False)
        terms.terms_df.to_csv(Terms.TERMS_FILE_PATH, index=False)
        print("Here is what we have after update: ")
        print(terms.class_df.groupby(["prefix"]).agg(
            class_prefix_count=pd.NamedAgg(column="prefix", aggfunc="count")
        ))
        print(terms.terms_df.groupby(["prefix"]).agg(
            term_prefix_count=pd.NamedAgg(column="prefix", aggfunc="count")
        ))
        return terms.terms_df, terms.class_df

    @staticmethod
    def update_register():
        """
        Register to keep a snapshot of the gbif extensions plus all the issue dates.
        """
        def _extract_prefix(url_col, identifier_col):
            def __def_extract_path(str_val):
                p = urlparse(str_val)
                return p.path
            path_str = __def_extract_path(url_col)
            test = re.search("/extension/(.*)/.*", path_str)
            if test:
                return test.group(1).replace("/1.0", "")
            else:
                test = re.search("/(core)/.*", path_str)
                if test and test.group(1):
                    return "dwc"
            path_str = __def_extract_path(identifier_col)
            test = re.search("/(dwc)/.*", path_str)
            if test:
                return test.group(1)
            return None

        d = requests.get(Terms.GBIF_EXT).json()
        gbif_ext_df = pd.DataFrame.from_dict(d["extensions"])
        gbif_ext_df = gbif_ext_df[gbif_ext_df["isLatest"]]
        gbif_ext_df = gbif_ext_df[~(gbif_ext_df["title"] + gbif_ext_df["description"]).str.contains("deprecate", case=False)]
        # Note: Search url first so that urls like http://rs.gbif.org/extension/dwc/ChronometricAge_2024-03-11.xml results in prefix dwc
        gbif_ext_df["prefix"] = np.vectorize(_extract_prefix)(gbif_ext_df["url"], gbif_ext_df["identifier"])
        gbif_ext_df.to_csv(EXTENSION_REGISTER_PATH, index=False)
