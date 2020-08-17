# rona
Helper scripts for bioinformatics stuff.

Should include:

* COG submission script.
* ENA submission script.
* NRP Metadata conversion to master table.

# Installation

## Google Sheets 
These scripts heavily use Google Sheets to track information. You will need your own developer account and you
 will need to have a valid JSON files with all the various keys. 

https://console.developers.google.com/project?pli=1

# NRP2COG

NRP2COG merges disperate Google sheets with various information into a single master table.
The internal modules include: 

* ct_update: Update CT values
* lineage_update: Update Linaege values
* export_lineage: Export Linaege values
* update_metadata: Update metadata

For COG and other database submission, the most important modules are update_metadata and ct_update. 

lineage_update is run when there is updates from PEROBA. 

export_lineages are run on request (weekly usually) to feedback contextual data and lineage information.
