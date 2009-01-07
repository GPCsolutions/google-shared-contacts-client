Copyright 2008 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


* Contents

shared_contacts/shared_contacts.py
  Python script for managing the shared contacts of a domain

shared_contacts/outlook.csv
  example CSV file for python shared_contacts.py --import


* Installation

The script requires the GData Python client library version 1.2.3 or higher.
Download location:
  http://code.google.com/p/gdata-python-client/downloads/list
Installation procedure:
  http://code.google.com/apis/gdata/articles/python_client_lib.html


* Usage

Imports the contacts of your-ms-outlook-contacts-file.csv into the domain.
Writes the added or updated contacts to output-file.csv:
  python shared_contacts.py --admin=your-admin-login@your-domain.com --import=your-ms-outlook-contacts-file.csv --output=output-file.csv

Exports all contacts of the domain to export-file.csv:
  python shared_contacts.py --admin=your-admin-login@your-domain.com --export=export-file.csv

Deletes all contacts of the domain:
  python shared_contacts.py --admin=your-admin-login@your-domain.com --clear


* Requirements
- Python 2.4 or higher
- ElementTree Python library (builtin with Python 2.5 and higher):
  http://pypi.python.org/pypi/elementtree/
- GData Python client library version 1.2.3 or above; available at:
  http://code.google.com/p/gdata-python-client/
- the login and password of a Google Apps domain administrator account


* Links
- GData Python client library
  http://code.google.com/p/gdata-python-client/
- Google Apps APIs discussion group
  http://groups.google.com/group/google-apps-apis
- Script home page
  http://code.google.com/p/google-shared-contacts-client/
