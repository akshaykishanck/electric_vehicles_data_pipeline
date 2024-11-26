## Data pipeline for Electric Vehicles
The project aims to develop an efficient pipeline that can ingest, process, analyze and visualize the EV Population data in the state of Washington from 2010 to 2025.

## Source of data - 
https://catalog.data.gov/dataset/electric-vehicle-population-data

Metadata(from the above website)
{"@type": "dcat:Dataset", "accessLevel": "public", "contactPoint": {"@type": "vcard:Contact", "fn": "Department of Licensing", "hasEmail": "mailto:no-reply@data.wa.gov"}, "description": "This dataset shows the Battery Electric Vehicles (BEVs) and Plug-in Hybrid Electric Vehicles (PHEVs) that are currently registered through Washington State Department of Licensing (DOL).", "distribution": [{"@type": "dcat:Distribution", "downloadURL": "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv?accessType=DOWNLOAD", "mediaType": "text/csv"}, {"@type": "dcat:Distribution", "describedBy": "https://data.wa.gov/api/views/f6w7-q2d2/columns.rdf", "describedByType": "application/rdf+xml", "downloadURL": "https://data.wa.gov/api/views/f6w7-q2d2/rows.rdf?accessType=DOWNLOAD", "mediaType": "application/rdf+xml"}, {"@type": "dcat:Distribution", "describedBy": "https://data.wa.gov/api/views/f6w7-q2d2/columns.json", "describedByType": "application/json", "downloadURL": "https://data.wa.gov/api/views/f6w7-q2d2/rows.json?accessType=DOWNLOAD", "mediaType": "application/json"}, {"@type": "dcat:Distribution", "describedBy": "https://data.wa.gov/api/views/f6w7-q2d2/columns.xml", "describedByType": "application/xml", "downloadURL": "https://data.wa.gov/api/views/f6w7-q2d2/rows.xml?accessType=DOWNLOAD", "mediaType": "application/xml"}], "identifier": "https://data.wa.gov/api/views/f6w7-q2d2", "issued": "2023-10-19", "keyword": ["tesla", "leaf", "nissan", "model 3", "dol", "department of licensing", "green report", "ev", "evs", "phev", "phevs", "bev", "bevs", "electric", "hybrid", "vehicle", "plug-in", "volt", "bolt", "chevy", "chevrolet", "car", "environment", "clean energy", "population", "hybrids", "plug-ins", "vehicles", "cars", "energy", "nhtsa", "rao_open_data", "dol_open_data", "rao_ev", "rao_veh"], "landingPage": "https://data.wa.gov/d/f6w7-q2d2", "license": "http://opendatacommons.org/licenses/odbl/1.0/", "modified": "2024-11-22", "publisher": {"@type": "org:Organization", "name": "data.wa.gov"}, "theme": ["Transportation"], "title": "Electric Vehicle Population Data"}


## Features
- Downloads the raw data from the url above.
- Stores the raw data in a mongoDB collection.
- Applies transformations on the raw data and stores the filtered/transformed data.
- Runs noSQL queries on the cleaned data in the database and generates visualizations.
- Restarts the pipeline from the last checkpoint or from scratch, improving flexibility
- Logging the whole pipeline for monitoring and debugging.
 
## How to Run the Project
1. Clone the repository:
   ```bash
   git clone https://github.com/akshaykishanck/INFOI535-FinalProject.git
2. Go to the project
   ```bash
   cd INFOI535-FinalProject
3. Install the required packages:
   ```bash
   pip install -r requirements.txt
3. Setup MongoDB and start service:
   ```bash
   ./mongo_setup.sh
NOTE: The above code(3) works for Ubuntu users only. MacOS and Windows users please refer MongoDB documentation to install and start a MongoDB service.
4. Run the pipeline(from start, deletes all noSQL collections if present):
   ```bash 
   python main.py --reset_and_start
   ```
NOTE: If you want to run the pipeline from where it stopped use the command. The `current_stage` is tracked in `pipeline-checkpoint.json` in the `config` folder   
```
python main.py
```
that is, without the reset_and_start flag

