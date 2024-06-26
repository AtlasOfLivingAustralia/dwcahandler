"""
A script called from cli for eg:
   poetry run update-dwc-terms

or github actions before doing the build
WIP: Need to automatically pull vocabulary version date from tdwg github
     and find a way to update Readme if possible
    Do we need to implement pulling a specific version of vocab?? Still need to decide
"""

from dwcahandler.dwca.terms import Terms

# Need to populate the Dwc term version programmatically
DWC_TERM_VERSION = "2023-09-17"


def update_terms():
    """
    Call the update_dwc_terms to get the latest version of tdwg dwc terms
    Do we need to get a particular version of csv url to pass in??
    """
    Terms.update_dwc_terms()
