# encoding: utf-8
"""
Helper class to get all the DRSs which need aggregating. This information is gleaned from the JSON store.
"""
__author__ = 'Richard Smith'
__date__ = '10 Mar 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from ceda_elasticsearch_tools.elasticsearch import CEDAElasticsearchClient
from cci_tagger_json import DatasetJSONMappings
import re


class DRSAggregationInfo:
    def __init__(self, id, aggregate=True, wms=False):
        self.id = id
        self.aggregate = aggregate
        self.wms = wms

    def __repr__(self):
        return self.id


class DRSAggregation:

    def __init__(self, index):
        self.es = CEDAElasticsearchClient()
        self.query = {
            "query": {
                "term": {"is_published": "true"}
            },
            "size": 0,
            "aggs": {
                "drsid": {
                    "composite": {
                        "sources": [
                            {
                                "drs": {
                                    "terms": {
                                        "field": "drsId.keyword"
                                    }
                                }
                            },
                            {
                                "path": {
                                    "terms": {
                                        "field": "path.keyword"
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }

        self.drs_ids = []
        self.page = None
        self.index = index
        self.dataset_json = DatasetJSONMappings()

    def _add_after_key(self, after_key):
        self.query['aggs']['drsid']['composite']['after'] = after_key

    def _extract_drs_ids(self):

        ids_to_aggregate = []

        aggregation = self.page['aggregations']

        buckets = aggregation['drsid']['buckets']

        for bucket in buckets:
            id = bucket['key']['drs']
            path = bucket['key']['path']

            aggregation_filters = self.dataset_json.get_aggregations(path)

            # Move on if intent to aggregate not specified in JSON files
            if not aggregation_filters:
                continue

            for filter in aggregation_filters:
                m = re.match(filter['pattern'], id)
                if m:
                    ids_to_aggregate.append(
                        DRSAggregationInfo(
                            id=id,
                            wms=filter.get('wms', False)
                        )
                    )
                    break

        self.drs_ids.extend(ids_to_aggregate)

    def _get_page(self):
        self.page = self.es.search(index=self.index, body=self.query)

    def _scroll_aggregations(self):
        self._get_page()
        self._extract_drs_ids()

    def get_aggregations(self):
        """
        Get a list of drs identifiers for further processing
        :return: List of drs_ids
        """

        # Run first page
        self._scroll_aggregations()

        # Loop subsequent pages
        after_key = self.page['aggregations']['drsid'].get('after_key')

        while after_key:
            self._add_after_key(after_key)
            self._scroll_aggregations()
            after_key = self.page['aggregations']['drsid'].get('after_key')

        return self.drs_ids
