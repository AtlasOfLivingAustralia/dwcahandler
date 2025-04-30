import pandas as pd
from numpy import nan
from dwcahandler.dwca import DwcaHandler, Terms, NsPrefix


class TestTerms:
    """
    Test for terms
    """

    def test_list_dwc_terms(self):
        """
        Test that mandatory terms are present
        """
        df, class_df = DwcaHandler.list_terms()
        assert df.query('term == "occurrenceID"').shape[0] == 1
        assert df.query('term == "basisOfRecord"').shape[0] == 1
        assert df.query('term == "scientificName"').shape[0] == 1
        assert df.query('term == "decimalLatitude"').shape[0] == 1
        assert df.query('term == "decimalLongitude"').shape[0] == 1
        assert df.query('term == "eventDate"').shape[0] == 1
        assert len(class_df[class_df["class"] == "OCCURRENCE"]) == 1

    def test_update_dwc_list_terms(self, mocker):
        """
        Test that the terms are stored in expected format and deprecated terms are not brought over
        """
        expected_uri_list = ["http://rs.tdwg.org/dwc/terms/basisOfRecord",
                             "http://rs.tdwg.org/dwc/terms/occurrenceID",
                             "http://rs.tdwg.org/dwc/terms/scientificName"]
        expected_class_list = ["http://rs.tdwg.org/dwc/terms/Occurrence"]
        mocker.patch.object(Terms,
                            attribute="get_dwc_source_data",
                            return_value=pd.DataFrame
                            ({"term_localName": ["occurrenceID", "basisOfRecord", "scientificName", "oldTerm"],
                              "term_isDefinedBy": ["http://rs.tdwg.org/dwc/terms/",
                                                   "http://rs.tdwg.org/dwc/terms/",
                                                   "http://rs.tdwg.org/dwc/terms/",
                                                   "http://rs.tdwg.org/dwc/terms/"],
                              "term_deprecated": [nan, nan, nan, "true"],
                              "tdwgutility_organizedInClass": ["http://rs.tdwg.org/dwc/terms/Occurrence",
                                                               "http://rs.tdwg.org/dwc/terms/Occurrence",
                                                               "http://rs.tdwg.org/dwc/terms/Occurrence",
                                                               "http://rs.tdwg.org/dwc/terms/Occurrence"]}))
        mocker.patch('pandas.DataFrame.to_csv')
        mocker.patch('dwcahandler.dwca.terms.Terms.update_gbif_ext')
        return_terms_df, return_class_df = Terms.update_terms()
        return_dwc_terms_df = return_terms_df[return_terms_df.prefix.isin(['dwc']) & return_terms_df["uri"].isin(expected_uri_list)].copy().reset_index(drop=True)
        return_dwc_class_df = return_class_df[return_class_df.prefix.isin(['dwc']) & return_class_df["class_uri"].isin(expected_class_list)].copy().reset_index(drop=True)
        pd.testing.assert_frame_equal(left=return_dwc_terms_df,
                                      right=pd.DataFrame(
                                          {"prefix": [NsPrefix.DWC.value, NsPrefix.DWC.value, NsPrefix.DWC.value],
                                           "term": ["basisOfRecord", "occurrenceID", "scientificName"],
                                           "uri": expected_uri_list}),
                                      check_index_type=False,
                                      check_dtype=False)
        pd.testing.assert_frame_equal(left=return_dwc_class_df,
                                      right=pd.DataFrame({"prefix": [NsPrefix.DWC.value],
                                                          "class": ["OCCURRENCE"],
                                                          "class_uri": expected_class_list}),
                                      check_index_type=False,
                                      check_dtype=False)
