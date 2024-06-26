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

&nbsp;
### Setup

* Clone the repository. 
* If using pyenv, install the required python version and activate it locally
```bash
pyenv local <python version>
```
* Install the dependency in local virtual environment
```bash
poetry shell
poetry install
```

* To update the darwin core terms supported in dwcahandler package
```bash
poetry run update-dwc-terms
```
&nbsp;
### Build
To build dwcahandler package
```bash
poetry build
```
&nbsp;
### Installation

To use locally built package in a virtual environment for eg in preingestion or galaxias:
```bash
pip install <folder>/dwcahandler/dist/dwcahandler-<version>.tar.gz
```

However, to install published package from testpypi
```bash
pip install -i https://test.pypi.org/simple/ dwcahandler
```
&nbsp;
### Examples of dwcahandler usages:

* Create Darwin Core Archive from csv file
```python
from dwcahandler import CsvFileType
from dwcahandler import DwcaHandler
from dwcahandler import Eml

core_csv = CsvFileType(files=['/tmp/occurrence.csv'], type='occurrence', keys='occurrenceID')
ext_csvs = [CsvFileType(files=['/tmp/multimedia.csv'], type='multimedia')]

eml = Eml(dataset_name='Test Dataset',
          description='Dataset description',
          license='Creative Commons Attribution (International) (CC-BY 4.0 (Int) 4.0)',
          citation="test citation",
          rights="test rights")

DwcaHandler.create_dwca(core_csv=core_csv, ext_csv_list=ext_csvs, eml_content=eml, output_dwca_path='/tmp/dwca.zip')
```
&nbsp;
* Create Darwin Core Archive from pandas dataframe
```python
from dwcahandler import DwcaHandler
from dwcahandler.dwca import DataFrameType
from dwcahandler import Eml
import pandas as pd

core_df = pd.read_csv("/tmp/occurrence.csv")
core_frame = DataFrameType(df=core_df, type='occurrence', keys=['occurrenceID'])

ext_df = pd.read_csv("/tmp/multimedia.csv")
ext_frame = [DataFrameType(df=ext_df, type='multimedia')]

eml = Eml(dataset_name='Test Dataset',
          description='Dataset description',
          license='Creative Commons Attribution (International) (CC-BY 4.0 (Int) 4.0)',
          citation="test citation",
          rights="test rights")

DwcaHandler.create_dwca(core_csv=core_frame, ext_csv_list=ext_frame, eml_content=eml, output_dwca_path='/tmp/dwca.zip')
```
&nbsp;
* Merge Darwin Core Archive
```python
from dwcahandler import DwcaHandler

DwcaHandler.merge_dwca(dwca_file='/tmp/dwca.zip', delta_dwca_file=/tmp/delta-dwca.zip,
                       output_dwca_path='/tmp/new-dwca.zip', 
                       keys_lookup={'occurrence':'occurrenceID'})
```
&nbsp;
* Delete Rows from core file in Darwin Core Archive
```python
from dwcahandler import CsvFileType
from dwcahandler import DwcaHandler

delete_csv = CsvFileType(files=['/tmp/old-records.csv'], type='occurrence', keys='occurrenceID')

DwcaHandler.delete_records(dwca_file='/tmp/dwca.zip',
                           records_to_delete=delete_csv, 
                           output_dwca_path='/tmp/new-dwca.zip')
```
&nbsp;
* List darwin core terms that is supported in dwcahandler package
```python
from dwca import DwcaHandler

df = DwcaHandler.list_dwc_terms()
print(df)
```
&nbsp;
* Other usages may include subclassing the dwca class, modifying the core dataframe content and rebuilding the dwca.
```python
from dwcahandler import Dwca

class DerivedDwca(Dwca):
    """
    Derived class to perform other custom operations that is not included as part of the core operations
    """
    def _drop_columns(self):
        """
        Drop existing column in the core content
        """
        self.core_content.df_content.drop(columns=['column1', 'column2'], inplace=True)
        self._update_meta_fields(self.core_content)


dwca = DerivedDwca(dwca_file_loc='/tmp/dwca.zip')
dwca._extract_dwca()
dwca._drop_columns()
dwca._generate_eml()
dwca._generate_meta()
dwca._write_dwca('/tmp/newdwca.zip')

```