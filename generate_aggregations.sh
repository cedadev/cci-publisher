#!/usr/bin/env bash
# Start the virtual environment
echo "Loading JASPY"
module load jaspy
. venv/bin/activate


# Pull latest CCI JSON Tags
echo "Installing cci_tagger_json"
pip install git+https://github.com/cedadev/cci_tagger_json.git -U --no-deps

# Clone latest catalog
echo "Cloning latest catalog"
rm -rf cci_odp_catalog
git clone https://breezy.badc.rl.ac.uk/cci-odp/cci_odp_catalog.git
cd cci_odp_catalog
git checkout -b catalog/`date +"%Y-%m-%d"`
cd ..


# Set script off to create lotus jobs to generate the aggregations
echo "Starting script to generate aggregations"
python cci_publisher/scripts/publish_aggregations.py --lotus

echo "Use command: squeue --user=$USER to check progress of the scripts"
