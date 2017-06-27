## Write a TSV for a given entity in a workspace
This script writes a TSV file for a given entity when that entity is too large to download from the GUI.

Run this as follows (from the main directory):
```./run.sh entity_to_tsv_for_large_data/entity_to_tsv_for_large_data.py -p <workspace project> -n <workspace name> -e <entity type, e.g. sample> -o <output_tsv_file_name>```