# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '13 Mar 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from configparser import ConfigParser
from cci_publisher.utils import DRSAggregation, write_catalog, get_all_catalog_files, get_state_store
from tqdm import tqdm
from .drs_dataset import DRSDataset
import importlib.util
import subprocess
import os


class CCIPublisher():

    def __init__(self, args, datasets, config=None):
        self.args = args

        self.datasets = datasets

        self.conf = config
        if not config:
            self._parse_config()

        self.state = get_state_store(self.conf)

        print(f'Total Datasets to process: {len(self.datasets)}')

    def _parse_config(self):
        """
        Parse config file
        """
        self.conf = ConfigParser()
        self.conf.read(self.args.config)

    def publish_datasets(self):
        """
        Generate the THREDDS catalog files for the published CCI datasets
        """

        for dataset in tqdm(self.datasets, desc='Generating catalog records'):

            if self.args.lotus:

                # Create lotus job
                script_path = importlib.util.find_spec('cci_publisher.scripts.aggregate').origin
                script_dir = os.path.dirname(script_path)
                task = f'{script_dir}/publish_aggregations.sh {script_path} -d {dataset.id}'

                # Add optional flags
                if self.args.force:
                    task = f'{task} --force'
                if dataset.wms:
                    task = f'{task} --wms'

                # Submit job
                command = f'sbatch --time 24:00:00 -e errors/{dataset.id}.err {task}'
                print(command)
                subprocess.call(command, shell=True)

            else:
                ds = DRSDataset(dataset.id, self.state, self.conf, force=self.args.force, wms=dataset.wms)
                ds.publish()

    def unpublish_datasets(self):
        """
        Remove catalog files and aggregation NCML where the dataset
        is no longer 'published' as defined by MOLES export tags
        """

        if self.args.datasets == 'all':
            dataset_list = self.datasets
        else:
            dataset_list = DRSAggregation(self.conf.get('elasticsearch','collections_index')).get_aggregations()

        ids_on_disk = {file.stem for file in get_all_catalog_files(self.conf.get('output','thredds_catalog_repo_path'))}

        ids_to_delete = ids_on_disk - {ds.id for ds in dataset_list}

        print(f'Aggregations on disk: {len(ids_on_disk)}')
        print(f'Aggregations to delete: {len(ids_to_delete)}')

        for record in ids_to_delete:
            ds = DRSDataset(record, self.state, self.conf)
            ds.unpublish()

        self.state.clear_unused(ids_to_delete)