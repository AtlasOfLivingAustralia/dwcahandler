## dwcahandler

### About
Python package to handle Darwin Core Archive (DwCA) operations. This includes creating a DwCA zip file from csv, reading a DwCA, merge two DwCAs, validate DwCA and delete records from DwCA based on one or more key columns

### Motivation
This package was developed from a module in ALA's data preingestion to produce a DwCA for pipelines ingestion. 
ALA receive different forms of data from various data providers in the form of CSV and text files, API harvest and DwCA, this is needed to package up the data into DwCA.

The operations provided by dwcahandler includes creating a dwca from csv/text file, merge 2 dwcas, delete records in dwca and perform core key validations like testing duplicates of one or more keys, empty and duplicate keys.  

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

* To import the darwin core and all the gbif extensions class row types and terms into dwcahandler
```bash
poetry run update-terms
```
&nbsp;
### Build
To build dwcahandler package
```bash
poetry build
```
&nbsp;
### Installation

Install published package
```bash
pip install dwcahandler
```

To use locally built package in a virtual environment:
```bash
pip install <folder>/dwcahandler/dist/dwcahandler-<version>.tar.gz
```

To install published package from testpypi
```bash
pip install -i https://test.pypi.org/simple/ dwcahandler
```
&nbsp;
### DwcaHandler is currently supporting the latest gbif extensions. 
### DwCA with the following extensions that have been ingested and tested in ALA are:
* Darwin Core Terms and Class RowTypes
* Simple Multimedia https://rs.gbif.org/extension/gbif/1.0/multimedia.xml
* Extended Measurement Or Fact http://rs.iobis.org/obis/terms/ExtendedMeasurementOrFact

#### Terms
* Terms are listed in [terms.csv](src/dwcahandler/dwca/terms/terms.csv)
```python
from dwcahandler import DwcaHandler

df_terms, df_class = DwcaHandler.list_terms()
print(df_terms, df_class)
```

#### Class
* Listed in [class-rowtype.csv](src/dwcahandler/dwca/terms/class-rowtype.csv)
* Used in MetaElementTypes class enum name:
```python 
from dwcahandler import MetaElementTypes

print(MetaElementTypes.OCCURRENCE)
print(MetaElementTypes.MULTIMEDIA)
```

To list all the Class Rowtypes
```python
from dwcahandler import DwcaHandler

DwcaHandler.list_class_rowtypes()
```
&nbsp;
### Examples of dwcahandler usages:

* Create Darwin Core Archive from csv file. 
* Keys in core content are used as id/core id for Dwca with extensions and must be supplied in the data for core and extensions
* If core data have more than 1 key (for eg: institutionCode, collectionCode and catalogNumber), resulting dwca would generate id/core id for extension
* Validation is performed to make sure that the keys are unique in the core of the Dwca by default
* If keys are supplied for the content extension, the validation will be run to check the uniqueness of the keys in the content
* If keys are not provided, the default keys is eventID for event content and occurrenceID for occurrence content
* In creating a dwca with multimedia extension, provide format and type values in the Simple Multimedia extension, otherwise, dwcahandler will attempt to fill these info by guessing the mimetype from url.
* For convenience, if occurrence text file contain dwc term [associatedMedia](https://dwc.tdwg.org/terms/#dwc:associatedMedia) and no multimedia extension is supplied, dwcahandler attempts to extract out the multimedia url from associatedMedia into [simple multimedia extemsion](https://rs.gbif.org/extension/gbif/1.0/multimedia.xml).
```python
from dwcahandler import ContentData
from dwcahandler import DwcaHandler
from dwcahandler import MetaElementTypes
from dwcahandler import Eml

core_csv = ContentData(data=['/tmp/occurrence.csv'], type=MetaElementTypes.OCCURRENCE, keys=['occurrenceID'])
ext_csvs = [ContentData(data=['/tmp/multimedia.csv'], type=MetaElementTypes.MULTIMEDIA)]

eml = Eml(dataset_name='Test Dataset',
          description='Dataset description',
          license='Creative Commons Attribution (International) (CC-BY 4.0 (Int) 4.0)',
          citation="test citation",
          rights="test rights")

DwcaHandler.create_dwca(core_csv=core_csv, ext_csv_list=ext_csvs, eml_content=eml, output_dwca='/tmp/dwca.zip')
```
&nbsp;
* Create Darwin Core Archive from pandas dataframe
* In creating a dwca with multimedia extension, provide format and type values in the Simple Multimedia extension, otherwise, dwcahandler will attempt to fill these info by guessing the mimetype from url.

```python
from dwcahandler import DwcaHandler
from dwcahandler.dwca import ContentData
from dwcahandler import MetaElementTypes
from dwcahandler import Eml
import pandas as pd

core_df = pd.read_csv("/tmp/occurrence.csv")
core_frame = ContentData(data=core_df, type=MetaElementTypes.OCCURRENCE, keys=['occurrenceID'])

ext_df = pd.read_csv("/tmp/multimedia.csv")
ext_frame = [ContentData(data=ext_df, type=MetaElementTypes.MULTIMEDIA)]

eml = Eml(dataset_name='Test Dataset',
          description='Dataset description',
          license='Creative Commons Attribution (International) (CC-BY 4.0 (Int) 4.0)',
          citation="test citation",
          rights="test rights")

DwcaHandler.create_dwca(core_csv=core_frame, ext_csv_list=ext_frame, eml_content=eml, output_dwca='/tmp/dwca.zip')
```
&nbsp;
* Convenient helper function to build Darwin Core Archive from a list of csv files.
* Build event core DwCA if event.txt file is supplied, otherwise, occurrence core DwCA if occurrence.txt is supplied. 
* Raises error if neither event.txt nor occurrence.txt is in the list
* Class row types are determined by file names of the text files.
* If no content keys provided, the default keys are eventID for event content and occurrenceID for occurrence content
* Delimiter for txt files are comma delimiter by default. For tab delimiter, supply CsvEncoding
```python
from dwcahandler import DwcaHandler
from dwcahandler import Eml

eml = Eml(dataset_name='Test Dataset',
          description='Dataset description',
          license='Creative Commons Attribution (International) (CC-BY 4.0 (Int) 4.0)',
          citation="test citation",
          rights="test rights")

DwcaHandler.create_dwca_from_file_list(files=["/tmp/event.csv", "/tmp/occurrence.csv"],  eml_content=eml, output_dwca='/tmp/dwca.zip')
```
&nbsp;
* Convenient helper function to create Darwin Core Archive from csv files in a zip files.
* Build event core DwCA if event.txt file is supplied, otherwise, occurrence core DwCA if occurrence.txt is supplied in the zip file
* Raises error if neither event.txt nor occurrence.txt is in the zip file
* Class row types are determined by file names of the text files.
* If no content keys provided, the default keys are eventID for event content and occurrenceID for occurrence content.
* Delimiter for txt files are comma delimiter by default. For tab delimiter, supply CsvEncoding
```python
from dwcahandler import DwcaHandler
from dwcahandler import Eml

eml = Eml(dataset_name='Test Dataset',
          description='Dataset description',
          license='Creative Commons Attribution (International) (CC-BY 4.0 (Int) 4.0)',
          citation="test citation",
          rights="test rights")

DwcaHandler.create_dwca_from_zip_content(zip_file="/tmp/txt_files.zip",  eml_content=eml, output_dwca='/tmp/dwca.zip')
```
&nbsp;
* Merge two Darwin Core Archives into a single file 
* Set extension sync to True to remove existing extension records before merging. Default for extension sync is False
```python
from dwcahandler import DwcaHandler, MetaElementTypes

DwcaHandler.merge_dwca(dwca_file='/tmp/dwca.zip', delta_dwca_file='/tmp/delta-dwca.zip',
                       output_dwca='/tmp/new-dwca.zip', 
                       keys_lookup={MetaElementTypes.OCCURRENCE:['occurrenceID']})
```
&nbsp;
* Delete Rows from core file in Darwin Core Archive
```python
from dwcahandler import ContentData
from dwcahandler import DwcaHandler, MetaElementTypes

delete_csv = ContentData(data=['/tmp/old-records.csv'], type=MetaElementTypes.OCCURRENCE, keys=['occurrenceID'])

DwcaHandler.delete_records(dwca_file='/tmp/dwca.zip',
                           records_to_delete=delete_csv,
                           output_dwca='/tmp/new-dwca.zip')
```
&nbsp;
