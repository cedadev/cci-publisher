# encoding: utf-8
"""
Script to generate the aggregation for a single dataset.
To be used in a batch pattern
"""
__author__ = 'Richard Smith'
__date__ = '21 May 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from cci_publisher.publisher.drs_dataset import DRSDataset
from cci_publisher.utils import get_state_store

import argparse
import os
from configparser import ConfigParser


def main():

    base_path = os.path.dirname(__file__)

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', dest='dataset', help='Dataset ID to aggregate')
    parser.add_argument('--wms', action='store_true', help='Boolean to determine whether to generate wms link')
    parser.add_argument('--conf', help='config file', default=os.path.join(base_path, '../config/cci_publisher_config.ini'))
    parser.add_argument('--force', action='store_true', help='force generation of aggregation even if no state change')

    args = parser.parse_args()

    conf = ConfigParser()
    conf.read(args.conf)

    state = get_state_store(conf)

    ds = DRSDataset(args.dataset, state, conf, force=args.force, wms=args.wms)
    ds.publish()


if __name__ == '__main__':
    main()