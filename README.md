# POLARE SCRAPPER

This is step by step guide to Political Entities import processes.
## 1. Pipeline Overview
* Install dependencies
* Scrap congressmen
* Scrap senators
* Scrap parties
* Run Congressman notebook
* Run Senator notebook
* Consolidate imports

## 2. The Project

The following defines the current project structure tree:

```
- polare_scrapper/
    - datasets/
        - camara
        - deprecated
        - senado
        - slp/
            - agents/
            - memberships/
            - organizations/
            - posts/
        - tse
    - aux.py    
    - Congressmen.ipynb
    - Senators.ipynb
    - spider_congressman_with_memberships.py
    - spider_political_parties.py
    - spider_senator_with_memberships.py
    - requirements.txt
    - README.md
    - .gitignore
```
### aux.py

File holding utility functions

### Congressmen.ipynb

This is a Jupiter notebook that handles JSON file conversion from datasets/camara/json to the output directory datasets/slp

Activate notebook by running at project's root directory:
> jupyter notebook

Execute each cell in order to create table like CSV files holding Org Ontology entities.

### Senators.ipynb

This is a Jupiter notebook that handles JSON file conversion from datasets/senado/json to the output directory datasets/slp

Activate notebook by running at project's root directory:
> jupyter notebook

Execute each cell in order to create table like CSV files holding Org Ontology entities.

### spider\_congressman\_with\_memberships.py

Scraps two versions of API provided by dadosabertos and Camara dos Deputados.

Invoke using the following command:
> scrapy runspider spider_congressman_with_memberships.py -o datasets/camara/json/congressman_with_memberships-55.json  -a legislatura=55

This procedure first queries a new API for all congressmen for given legislatura (term). It then queries the older API for the details of each of then. Saving information relative to the Entities: Agents, Memberhips, Roles and Posts.

### spider\_political\_parties.py

Fetches tse data

### spider\_senator\_with_memberships.py

Scraps two versions of API provided by dadosabertos and Camara dos Deputados.

Invoke using the following command:
> spider_senator_with_memberships.py -o datasets/sen/json/senator_with_memberships-55_56.json -a legislatura=55

This procedure first queries a new API for all senators for given two legislaturas (The senator term spans more then one). It saves information in json format relative to the Entities: Agents, Memberhips, Roles and Posts.


### requirements.txt

Defines the required project libs that must be installed. 

Install by running:
> pip install -r requirements.txt