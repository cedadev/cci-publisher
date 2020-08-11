# encoding: utf-8
"""
Script to generate the root catalog from the dataset specific catalogs
"""
__author__ = 'Richard Smith'
__date__ = '21 May 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from cci_publisher.datasets.create_catalog import CCICatalogBuilder
import argparse
from cci_publisher.utils import get_all_catalog_files, write_catalog


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--catalog-dir', help='Path to catalog records')

    args = parser.parse_args()

    catalog_list = get_all_catalog_files(args.catalog_dir)

    catalog_builder = CCICatalogBuilder()

    # Generate root catalog file
    catalog = catalog_builder.root_catalog(catalog_list, args.catalog_dir)

    write_catalog(catalog, f'{args.catalog_dir}/catalog.xml')


if __name__ == '__main__':
    main()
