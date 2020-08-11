# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '13 Mar 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from ceda_elasticsearch_tools.elasticsearch import CEDAElasticsearchClient
from elasticsearch.helpers import scan
from cci_publisher.datasets import ThreddsXMLDataset
import os
from cci_publisher.datasets.create_catalog import CCICatalogBuilder
from cci_publisher.utils import write_catalog, get_aggregation_subdir


class DRSDataset:

    def __init__(self, dataset_id, state, conf, force=False, wms=False, aggregate=True):

        # Preset values
        self.total_files = None
        self.results = []
        self.updated = False

        # Set main values
        self.id = dataset_id
        self.conf = conf
        self.builder = CCICatalogBuilder()
        self.state = state
        self.es = CEDAElasticsearchClient(headers={'x-api-key':conf.get('elasticsearch','api_key')})
        self.files_index = self.conf.get('elasticsearch','files_index')
        self.force = force
        self.wms = wms
        self.aggregate = aggregate

        # Get processed attributes
        self.get_file_count()
        self._get_state()
        self.catalog_path = f'{self.conf.get("output", "thredds_catalog_repo_path")}/data/catalog/datasets/{self.id}.xml'
        self.ncml_root = f'{self.conf.get("output", "thredds_catalog_repo_path")}/data/aggregations/'

    def _get_state(self):
        self.updated = self.state.has_updated(
            dataset=self.id,
            file_count=self.total_files,
            aggregate=self.aggregate,
            wms=self.wms
        )

    def _get_query(self):
        return {
            'query': {
                'bool': {
                    'must': [
                        {
                            'term': {
                                'projects.opensearch.drsId.keyword': self.id
                            }
                        },
                        {
                            'term': {
                                'info.format.keyword': {
                                    'value': 'NetCDF'
                                }
                            }
                        }
                    ]
                }
            }
        }

    def get_file_count(self):
        query = self._get_query()
        self.total_files = self.es.count(index=self.files_index, body=query)['count']

    def get_file_list(self):
        """
        Query elasticsearch for all netcdf files which match dataset ID

        :param dataset_id:
        :return:
        """

        query = self._get_query()

        # Reduce data sent back in scan
        query['_source'] = {
            'includes': ['info.directory', 'info.name', 'info.size']
        }

        results = scan(self.es, query=query, index=self.files_index)

        self.results = [
            {
                'directory': result['_source']['info']['directory'],
                'name': result['_source']['info']['name'],
                'size': result['_source']['info']['size']

            }
            for result in results
        ]

    def build_catalog(self):

        # If there is a change compared to state store or told to create regardless
        # and > 0 file
        if (self.updated or self.force) and self.total_files:
            # Get the file list to work with
            self.get_file_list()

            catalog = self.builder.dataset_catalog(self.results, ds_id=self.id, opendap=True)

            # Write the catalog file to disk
            write_catalog(catalog, self.catalog_path)

    def delete_catalog(self):
        os.remove(self.catalog_path)

    def build_aggregation(self):

        # There need to be files and the aggregation flag set. Then either there needs to be a change
        # or the force flag is set
        if all([self.aggregate, self.total_files]) and (self.updated or self.force):
            netcdf_files = [
                os.path.join(result['directory'], result['name'])
                for result in self.results
            ]

            # Prepare the Dataset Object
            xml_dataset = ThreddsXMLDataset(
                aggregations_dir=self.conf.get('remote', 'aggregations_dir'),
                thredds_server=self.conf.get('remote', 'thredds_server'),
                do_wcs=True,
                netcdf_files=netcdf_files
            )

            # Read in the catalog file to modify
            xml_dataset.read(self.catalog_path)

            # Add the aggregations and wms (if requested)
            xml_dataset.all_changes(create_aggs=True, add_wms=self.wms)

            # Write out the changes
            xml_dataset.write(self.catalog_path, agg_dir=self.ncml_root)

    def delete_aggregation(self):

        agg_subdir = get_aggregation_subdir(self.id)
        aggregation_path = os.path.join(self.ncml_root, agg_subdir, f'{self.id}.ncml')
        os.remove(aggregation_path)

    def publish(self):

        self.build_catalog()

        self.build_aggregation()

        if self.updated:
            self.state.update(self.id, self.total_files, self.aggregate, self.wms)

    def unpublish(self):

        self.delete_catalog()

        self.delete_aggregation()
