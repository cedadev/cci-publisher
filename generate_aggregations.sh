#!/usr/bin/env bash
# Start the virtual environment
echo "Loading JASPY"
module load jaspy
. venv/bin/activate

# Create slurm errors output
echo "Creating slurm errors directoru"
mkdir -p errors
echo


# Pull latest CCI JSON Tags
echo "Cloning latest cci_tagger_json"
rm -rf cci_tagger_json
git clone https://breezy.badc.rl.ac.uk/rsmith013/cci_tagger_json

echo
export JSON_TAGGER_ROOT="cci_tagger_json"
echo "Reading json files from: "$JSON_TAGGER_ROOT
echo

# Clone latest catalog
echo "Cloning latest catalog"
rm -rf cci_odp_catalog
git clone https://breezy.badc.rl.ac.uk/cci-odp/cci_odp_catalog.git
cd cci_odp_catalog
git checkout -b catalog/`date +"%Y-%m-%d"`
cd ..
echo


# Set script off to create lotus jobs to generate the aggregations
echo "Starting script to generate aggregations"
python cci_publisher/scripts/publish_aggregations.py --lotus

echo "Use command: squeue --user=$USER to check progress of the scripts"
