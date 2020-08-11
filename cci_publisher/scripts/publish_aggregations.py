# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '03 Feb 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

import argparse
from cci_publisher.utils import DRSAggregation, EmptyIsTrue, DRSAggregationInfo
from cci_publisher.publisher import CCIPublisher
from configparser import ConfigParser
import os


def get_args():
    base_path = os.path.dirname(__file__)

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-d',
        '--datasets',
        dest='datasets',
        nargs='+',
        default='all',
        action=EmptyIsTrue,
        help='Names of input datasets to generate catalogs. (DRS ids). Default: %(default)s'
    )

    parser.add_argument(
        '--wms',
        action='store_true',
        help='Add WMS and WCS endpoint for aggregations',
    )

    parser.add_argument(
        '--config',
        help='Path to config file',
        default=os.path.join(base_path, '../config/cci_publisher_config.ini')
    )

    parser.add_argument(
        '-f',
        '--force',
        dest='force',
        action='store_true',
        help='Force changes even if state store says it has been done before',
    )

    parser.add_argument(
        '--lotus',
        dest='lotus',
        action='store_true',
        help='Generate aggregations on lotus'
    )

    parser.add_argument(
        '--skip-unpublish',
        dest='skip_unpublish',
        action='store_true',
        help='Do not run the unpublish step'
    )

    parser.add_argument(
        '--skip-publish',
        dest='skip_publish',
        action='store_true',
        help='Do not run the publish step'
    )

    args = parser.parse_args()

    return args


def main():
    args = get_args()
    conf = ConfigParser()
    conf.read(args.config)

    if args.datasets == 'all':
        datasets = DRSAggregation(conf.get('elasticsearch', 'collections_index')).get_aggregations()
    else:
        datasets = [DRSAggregationInfo(ds, wms=args.wms) for ds in args.datasets]

    publisher = CCIPublisher(args, datasets, config=conf)

    # Generate catalog records and aggregations
    if not args.skip_publish:
        publisher.publish_datasets()

    # Remove unpublished catalog records
    if not args.skip_unpublish:
        publisher.unpublish_datasets()

    # Build root catalog:

    # Push catalog and aggregation files to THREDDS server

    # Reload THREDDS server to reflect changes


if __name__ == '__main__':
    main()
