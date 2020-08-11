# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '05 May 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from ceda_elasticsearch_tools.elasticsearch import CEDAElasticsearchClient
import elasticsearch
import hashlib


class AggregationState:
    """
    Convenience class to handle aggregation state store response
    """
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class StateStore:
    """
    Interface to the the state store for CCI Aggregations.
    """

    def __init__(self, index, **kwargs):
        self.session = CEDAElasticsearchClient(**kwargs)
        self.index = index

        # Create and index, if it doesn't exist
        if not self.session.indices.exists(self.index):
            self.session.indices.create(index=self.index)

    @staticmethod
    def _generate_id(string):
        """
        Convenience method to generate a unique hash per dataset
        :param string: Input string
        :return: (Str) sha1 hex
        """
        return hashlib.sha1(string.encode('utf-8')).hexdigest()

    def add_row(self, dataset, count, aggregate, wms):
        """
        Add the specified dataset
        :param dataset: DRS
        :param count: Total fiels
        :param aggregate: Boolean
        :param wms: Boolean
        """

        self.session.index(index=self.index, id=self._generate_id(dataset), body={
            'id': dataset,
            'file_count': count,
            'aggregate': aggregate,
            'wms': wms
        })

    def delete_row(self, dataset):
        """
        Delete the specified dataset
        :param dataset: DRS
        """
        try:
            self.session.delete(index=self.index, id=self._generate_id(dataset))
        except elasticsearch.exceptions.NotFoundError:
            pass

    def get_dataset(self, dataset):
        """
        Get the specified dataset
        :param dataset: DRS
        :return: AggregationState | None
        """

        try:
            response = self.session.get(index=self.index, id=self._generate_id(dataset))
            return AggregationState(**response['_source'])

        except elasticsearch.exceptions.NotFoundError:
            return None

    def has_updated(self, dataset, file_count, aggregate, wms):
        """
        Check if the details have changed and so we need to run the
        aggregation again.
        :param dataset: DRS
        :param file_count: Total files
        :param aggregate: Boolean
        :param wms: Boolean
        :return: Boolean
        """
        aggregation = self.get_dataset(dataset)

        # No match so this is a new aggregation and need to process it
        if not aggregation:
            return True

        # Check all the fields against the store.
        if not all([
            aggregation.file_count == file_count,
            aggregation.aggregate == aggregate,
            aggregation.wms == wms
        ]):
            return True

        # The dataset is in the table and the fields are unchanged.
        return False

    def update(self, dataset, count, aggregate, wms):
        """
        Update details for a given dataset
        :param dataset: DRS
        :param count: total files
        :param aggregate: Boolean
        :param wms: Boolean
        """

        # Indexing to the same ID will update the row
        self.add_row(dataset, count, aggregate, wms)

    def clear_unused(self, ids_to_remove):
        """
        Clear ids which are no longer active aggregations
        :param ids_to_remove:
        """
        for id in ids_to_remove:
            self.delete_row(id)
