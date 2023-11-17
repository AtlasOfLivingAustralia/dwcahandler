## dwcahandler

### About
Python package to handle Darwin Core Archive (DwCA) operations. This includes creating a DwCA zip file from csv, reading a DwCA, merge two DwCAs, validate DwCA and delete records from DwCA based on one or more key columns

### Motivation
This package was developed from a module in ALA's data preingestion to produce a DwCA for pipelines ingestion. 
ALA receive different forms of data from various data providers in the form of CSV and text files, API harvest and DwCA, this is needed to package up the data into DwCA.

The operations provided by dwcahandler includes creating a dwca from csv/text file, merge 2 dwcas, delete records in dwca and perform core key validations like testing duplicates of one or more keys, empty and duplicate keys.  

The module uses and maintain the standard dwc terms from a point in time versioned copy of https://dwc.tdwg.org/terms/ and extensions like https://rs.gbif.org/extension/gbif/1.0/multimedia.xml. 


### Technologies

This package is developed in Python. Tested with Python 3.12, 3.11, 3.10 and 3.9


### Setup

* Clone the repository. 
* If using pyenv, install the required python version and activate it locally
```
pyenv local <python version>
```
* Install the dependency in local virtual environment
```
poetry shell
poetry install
```

### Build
To build dwcahandler package
```
poetry build
```


### Usage

To use locally built package in a virtual environment for eg in preingestion or galaxias:
```
pip install <folder>/dwcahandler/dist/dwcahandler-<version>.tar.gz
```

However, to install published package from testpypi
```
pip install -i https://test.pypi.org/simple/ dwcahandler
```

#### Examples of dwcahandler usages:

* Create Darwin Core Archive
```
from dwcahandler import CsvFileType
from dwcahandler import DwcaHandler

core_csv = CsvFileType(files=['/tmp/occurrence.csv'], type='occurrence', keys='occurrenceID')
ext_csvs = [CsvFileType(files=['/tmp/multimedia.csv'], type='multimedia')]

DwcaHandler.create_dwca(core_csv=core_csv, ext_csv_list=ext_csvs, output_dwca_path='/tmp/dwca.zip')
```


