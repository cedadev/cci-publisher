# CCI Publisher

Repository for generating Opendap aggregations for the CCI Project.
Opendap endpoints are provided via a thredds Dataset Scan element but more complex aggregations have to be constructed
independently.

This repo is to build those aggregations.

The catalogs are output into a git repo which serves as the source for the CCI THREDDS service. In the containerised
THREDDS, this repo is used to build the image ready for deployment.

## Generating Aggregations

For convenience a [wrapper script](generate_aggregations.sh) has been provided but the essential flow is:
1. Install the lastest [tag json](https://github.com/cedadev/cci_tagger_json) into the virtual environment.
2. Clone the latest [catalog repo](https://breezy.badc.rl.ac.uk/rsmith013/cci_odp_catalog)
3. Run the [script](cci_publisher/scripts/publish_aggregations.py) to farm out the aggregation generation

```bash
source generate_aggregations.sh
``` 

#### Notes
You will need to provide valid credentials to access gitlab and may need to `chmod +x generate_aggregations.sh`

## Post Generation

The aggregations will take a while to complete, some of the larger ones will take several hours.
Once all the aggregations have completed, you will need to check the changes in the catalog repo 
(you can use git status or git diff to see what has been changed).

Once you are happy with these changes, run [build_root_catalog.py](cci_publisher/scripts/build_root_catalog.py) to build the root catalog.

```bash
python cci_publisher/scripts/build_root_catalog.py --catalog-dir cci_odp_catalog/data/catalog
```

This should be a quick process as it is just listing the files and generating some xml

Once complete, and happy, push the new catalog. This will get picked up by the automatic THREDDS
deployment pipeline and containers with the new aggregations will be built.