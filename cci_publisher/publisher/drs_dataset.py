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
    """
    Class for a single dataset. Provides the publish and unpublish interface for
    a single DRS Dataset

    Attributes:
        total_files:    int     Total files in the DRS Dataset
        results:        list    List of files in the DRS Dataset
        updated:        bool    Has the aggregation been updated
        id:             str     DRS ID
        state:          AggregationState
        force:          bool    Ignore state when deciding to aggreate
        wms:            bool    Provide WMS access
        aggregate:      bool    Whether or not to aggregate dataset
        catalog_path:   str     xml Catalog file path
        ncml_root:      str     NCML file path
    """

    def __init__(self, dataset_id, state, conf, force=False, wms=False, aggregate=True):

        # Preset values
        self.total_files = None
        self.results = []
        self.updated = False

        # helper values
        self._conf = conf
        self._builder = CCICatalogBuilder()
        self._es = CEDAElasticsearchClient(headers={'x-api-key':conf.get('elasticsearch', 'api_key')})
        self._files_index = self._conf.get('elasticsearch', 'files_index')

        # Set main values
        self.id = dataset_id
        self.state = state
        self.force = force
        self.wms = wms
        self.aggregate = aggregate

        # Get processed attributes
        self._get_file_count()
        self._get_state()
        self.catalog_path = f'{self._conf.get("output", "thredds_catalog_repo_path")}/data/catalog/datasets/{self.id}.xml'
        self.ncml_root = f'{self._conf.get("output", "thredds_catalog_repo_path")}/data/aggregations/'

    def _get_state(self):
        """
        Find out if the DRS dataset is different compared to the last run.
        This avoids re-running compute intesive aggregations for datasets which
        have not changed

        :return: bool
        """
        self.updated = self.state.has_updated(
            dataset=self.id,
            file_count=self.total_files,
            aggregate=self.aggregate,
            wms=self.wms
        )

    def _get_query(self):
        """
        Returns the base query

        :return: es base query
        :rtype: dict
        """
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

    def _get_file_count(self):
        """
        Number of files in the dataset, according to files index
        """
        query = self._get_query()
        self.total_files = self._es.count(index=self._files_index, body=query)['count']

    def _get_file_list(self):
        """
        Query elasticsearch for all netCDF files which match dataset ID
        """

        query = self._get_query()

        # Reduce data sent back in scan
        query['_source'] = {
            'includes': ['info.directory', 'info.name', 'info.size']
        }

        results = scan(self._es, query=query, index=self._files_index)

        self.results = [
            {
                'directory': result['_source']['info']['directory'],
                'name': result['_source']['info']['name'],
                'size': result['_source']['info']['size']

            }
            for result in results
        ]

    def _build_catalog(self):
        """
        Build the catalog record
        """

        # If there is a change compared to state store or told to create regardless
        # and > 0 file
        if (self.updated or self.force) and self.total_files:
            # Get the file list to work with
            self._get_file_list()

            catalog = self._builder.dataset_catalog(ds_id=self.id, opendap=True)

            # Write the catalog file to disk
            write_catalog(catalog, self.catalog_path)
        else:
            print('Catalog already exists')

    def _delete_catalog(self):
        """
        Delete the catalog record
        """
        os.remove(self.catalog_path)

    def _build_aggregation(self):
        """
        Build the NCML aggregation
        """

        # There need to be files and the aggregation flag set. Then either there needs to be a change
        # or the force flag is set
        if all([self.aggregate, self.total_files]) and (self.updated or self.force):
            netcdf_files = [
                os.path.join(result['directory'], result['name'])
                for result in self.results
            ]

            # Prepare the Dataset Object
            xml_dataset = ThreddsXMLDataset(
                aggregations_dir=self._conf.get('remote', 'aggregations_dir'),
                thredds_server=self._conf.get('remote', 'thredds_server'),
                do_wcs=True,
                netcdf_files=netcdf_files
            )

            # Read in the catalog file to modify
            xml_dataset.read(self.catalog_path)

            # Add the aggregations and wms (if requested)
            xml_dataset.all_changes(create_aggs=True, add_wms=self.wms)

            # Write out the changes
            xml_dataset.write(self.catalog_path, agg_dir=self.ncml_root)

    def _delete_aggregation(self):
        """
        Delete aggregation file
        """

        agg_subdir = get_aggregation_subdir(self.id)
        aggregation_path = os.path.join(self.ncml_root, agg_subdir, f'{self.id}.ncml')
        os.remove(aggregation_path)

    def publish(self):
        """
        Generate an aggregation for the DRS Dataset
        """

        self._build_catalog()

        self._build_aggregation()

        if self.updated:
            self.state.update(self.id, self.total_files, self.aggregate, self.wms)

    def unpublish(self):
        """
        Remove all associated catalog files for the DRS Dataset
        """

        self._delete_catalog()

        self._delete_aggregation()
