import pandas as pd
from numpy import nan
from dwcahandler.dwca import DwcaHandler, Terms


class TestTerms:
    """
    Test for terms
    """

    def test_list_dwc_terms(self):
        """
        Test that mandatory terms are present
        """
        df = DwcaHandler.list_dwc_terms()
        assert df.query('term == "occurrenceID"').shape[0] == 1
        assert df.query('term == "basisOfRecord"').shape[0] == 1
        assert df.query('term == "scientificName"').shape[0] == 1
        assert df.query('term == "decimalLatitude"').shape[0] == 1
        assert df.query('term == "decimalLongitude"').shape[0] == 1
        assert df.query('term == "eventDate"').shape[0] == 1

    def test_update_list_terms(self, mocker):
        """
        Test that the terms are stored in expected format and deprecated terms are not brought over
        """
        mocker.patch('pandas.read_csv',
                     return_value=pd.DataFrame(
                         {"term_localName": ["occurrenceID", "basisOfRecord",
                                             "scientificName", "oldTerm"],
                          "term_isDefinedBy": ["http://rs.tdwg.org/dwc/terms/",
                                               "http://rs.tdwg.org/dwc/terms/",
                                               "http://rs.tdwg.org/dwc/terms/",
                                               "http://rs.tdwg.org/dwc/terms/"],
                          "term_deprecated": [nan, nan, nan, "true"]}))
        mocker.patch('pandas.DataFrame.to_csv')
        return_dwc_df = Terms.update_dwc_terms()
        pd.testing.assert_frame_equal(return_dwc_df,
                                      pd.DataFrame({"term": ["occurrenceID", "basisOfRecord",
                                                    "scientificName"],
                                                    "uri": ["http://rs.tdwg.org/dwc/terms/occurrenceID",
                                                            "http://rs.tdwg.org/dwc/terms/basisOfRecord",
                                                            "http://rs.tdwg.org/dwc/terms/scientificName"]}))
