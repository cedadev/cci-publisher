# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '10 Mar 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from cci_publisher.state_store.state_store import StateStore
import argparse
import pathlib
from .drs_id_aggregation import DRSAggregation, DRSAggregationInfo
import os


def write_catalog(catalog, output_path):
    """
    Write the catalog XML to disk
    """

    with open(output_path, 'w') as writer:
        writer.write(catalog)


class EmptyIsTrue(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if len(values) == 0:
            values = True
        setattr(namespace, self.dest, values)


def get_aggregation_subdir(id):
    """
    Generate the aggregation path.
    Id can either be a DRS or xml dataset filename.
    eg.
    esacci.CLOUD.mon.L3C.CLD_PRODUCTS.multi-sensor.multi-platform.AVHRR-AM.2-0.r1.xml
    OR
    esacci.CLOUD.mon.L3C.CLD_PRODUCTS.multi-sensor.multi-platform.AVHRR-AM.2-0.r1

    :param id: XML Dataset or DRS ID
    :return: aggregation subdir
    """
    items = id.lstrip('esacci.').rstrip('.xml').split('.')

    return os.path.join(*items)


def get_all_catalog_files(catalog_dir):
        """
        Get a list of all the generated catalog files
        :return: list of Pathlib paths to catalog records
        """

        path = pathlib.Path(f'{catalog_dir}/data/catalog/datasets/')
        return path.glob('esacci*.xml')


def get_state_store(config):
    index = config.get('elasticsearch', 'state_index')
    api_key = config.get('elasticsearch', 'api_key')
    return StateStore(index=index, headers={'x-api-key': api_key})