## Quick and dirty tool to help make use of DRS URIs as though you were using gsutil
This script acts as though you are using gsutil when you have a DRS URI in Terra/FireCloud. 

Usage:

* `python3 drsutil.py cp drs://dataguids.org/10c6e468-efdd-4a22-801a-e6b7746986e6 .`
   This is equivalent to having run the gsutil command: `gsutil cp gs://whatever_file_that_resolves_to .`
   In theory this should work just like gsutil if you replace `gsutil` with `python3 drsutil.py` and your google 
   bucket URL with your DRS URI.  
