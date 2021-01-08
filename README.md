# ronasub
Helper scripts for merging and submitting SARSCOV data to ENA, NCBI, COG

Should include:

* COG submission script.
* ENA submission script.
* Rapid submission script (Lightsub)
* NRP Metadata conversion to master table.

# Installation
Python3! 

```
pip install -r requirements.txt
```

# NRP2COG

NRP2COG merges disperate Google sheets with various information into a single master table.
The internal modules include: 

* ct_update: Update CT values
* lineage_update: Update Linaege values
* export_lineage: Export Lineage values
* update_metadata: Update metadata
* export_to_server: Export values to external server

For COG and other database submission, the most important modules are update_metadata and ct_update. 

lineage_update is run when there is updates from PEROBA. 

export_lineages are run on request (weekly usually) to feedback contextual data and lineage information.


## Configuration for NRP2COG
These scripts heavily use Google Sheets to track information. You will need your own developer account and you
 will need to have a valid JSON files with all the various keys (see credentials_template.json for an example). 
 
https://console.developers.google.com/project?pli=1

There is usually location and a flag to pass your own path to the config file. If you are contributing to 
the QIB SARSCOV2 get in touch with @happykhan, who can issue the correct keys. 

# COGSUB 
COGSUB submits the metadata and sequences to COG UK. 

## Configuration for COGSUB
You will need the google sheet credentials (as above). You will also need to set up a config JSON file for COG/Majora (see Majora_template.json for an example)

## LIGHTSUB 
LIGHTSUB submits the rapid sequences to COG UK. The process to specify an output data directory, which is uploaded to COG. And then use MAJORA API client to link these files in the database. 

```
USAGE:
python cogsub/lightsub.py my_data_dir my_run_name --ont 
```
Then run the ```library.sh``` and ```run.sh```

### Configuration for LIGHTSUB
You will need the majora client installed. 

```
pip install git+https://github.com/samstudio8/ocarina.git
```
