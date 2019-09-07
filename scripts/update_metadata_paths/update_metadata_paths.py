##!/usr/bin/env python
from common import *
import tempfile

def main():
    setup()

    # The main argument parser
    parser = DefaultArgsParser(description="Replace old bucket paths in workspace metadata with new.")

    # Core application arguments
    parser.add_argument('-p', '--project', dest='ws_project', action='store', required=True, help='Workspace project')
    parser.add_argument('-n', '--name', dest='ws_name', action='store', required=True, help='Workspace name')
    parser.add_argument('-r', '--replace', dest='replacements', action='append', required=True, help='Old and new bucket names to be used for replacing existing data - the format this expects is "<old_bucket_name>=<new_bucket_name>".')

    args = parser.parse_args()


    entity_types_response = firecloud_api.list_entity_types(args.ws_project, args.ws_name)
    if entity_types_response.status_code != 200:
        print("Unable to fetch entity types.")
        exit(1)
    
    entity_types = entity_types_response.json().keys()

    for entity_type in entity_types:
        if str(entity_type).endswith("_set"):
            continue

        print("Getting TSV for entity type: " + entity_type)
        entity_tsv_response = firecloud_api.get_entities_tsv(args.ws_project, args.ws_name,entity_type)

        if entity_tsv_response.status_code != 200:
            print("Unable to get TSV for entity type: " + entity_type)
            exit(1)
        
        replaced_entities_tsv_path = "/tmp/%s.tsv"%(entity_type)
        with open(replaced_entities_tsv_path, 'w') as replaced_entities_tsv:
            replaced_text = entity_tsv_response.text
            for replacement in args.replacements:
                old, new = replacement.split("=")
                old_path = "gs://"+old+"/"
                new_path = "gs://"+new+"/"
                replaced_text = replaced_text.replace(old_path, new_path)
            
            replaced_entities_tsv.write(replaced_text)
        

        print("\tUploading modified TSV...")
        firecloud_api.upload_entities_tsv(args.ws_project, args.ws_name, replaced_entities_tsv_path)
        print("\tdone.")

if __name__ == "__main__":
    main()
