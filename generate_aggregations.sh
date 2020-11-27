#!/usr/bin/env bash
# Start the virtual environment
module load jaspy
. venv/bin/activate

# Pull latest CCI JSON Tags
pip install git+https://github.com/cedadev/cci_tagger_json.git -U --no-deps

# Clone latest catalog
git clone https://breezy.badc.rl.ac.uk/cci-odp/cci_odp_catalog.git

# Set script off to create lotus jobs to generate the aggregations
python cci_publisher/scripts/publish_aggregations.py --lotus

echo "Use command: squeue --user=$USER to check progress of the scripts"
