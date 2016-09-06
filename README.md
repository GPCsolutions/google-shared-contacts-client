A script to manage Google Apps Domain Shared Contacts.

# Contents

Python script for managing the shared contacts/profiles of a domain: [shared_contacts_profiles.py](shared_contacts_profiles.py)


Example CSV file for ```python shared_contacts_profiles.py --import```: [outlook.csv](outlook.csv)

# Installation

```
pip install requirements.txt
```

# Setup

1. Create a project in the [Google Developers Console](https://console.developers.google.com)
2. Enable the Contacts API
3. Create an OAuth client ID credential
4. Save the json file as ```client_secret.json``` alongside the script

# Usage

Imports the contacts of ```your-ms-outlook-contacts-file.csv``` into the domain.
Writes the added or updated contacts to output-file.csv:
  ```
  python shared_contacts_profiles.py --admin=your-admin-login@your-domain.com --import=your-ms-outlook-contacts-file.csv --output=output-file.csv
  ```

Exports all contacts of the domain to export-file.csv:
  ```
  python shared_contacts_profiles.py --admin=your-admin-login@your-domain.com --export=export-file.csv
  ```

Deletes all contacts of the domain:
  ```
  python shared_contacts_profiles.py --admin=your-admin-login@your-domain.com --clear
  ```


# Requirements

- Python 2.6 or higher

- oauth2client available at:  
  https://github.com/google/oauth2client

- GData Python client library available at:  
  https://github.com/google/gdata-python-client

- a Google Apps domain administrator account


# Links

- oauth2client library  
  https://oauth2client.readthedocs.io/en/latest/index.html

- GData Python client library  
  https://developers.google.com/gdata/articles/python_client_lib  
  https://pythonhosted.org/gdata

- Script home page  
  https://github.com/GPCsolutions/google-shared-contacts-client

# License

```
Copyright 2008 Google Inc.
Copyright 2016 GPC.solutions

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
