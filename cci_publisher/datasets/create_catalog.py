# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '25 Feb 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from tds_utils.create_catalog import CatalogBuilder, AccessMethod, DatasetRoot, AvailableServices, Aggregation, get_catalog_name, CatalogRef
from collections import namedtuple
import os
from jinja2 import Environment, PackageLoader


# Not used?
Property = namedtuple('Property', ('name', 'value'))
Variable = namedtuple('Variable', ['name', 'vocabulary_name', 'units'])


class Dataset:
    """
    Not used?
    """

    def __init__(self, id, name=None, urlpath=None, properties=[], access_methods=[], size=None):
        self.name = name if name else id
        self.id = id
        self.urlpath = urlpath
        self.properties = properties
        self.access_methods = access_methods
        self.dataSize = size


class CCICatalogBuilder(CatalogBuilder):
    DS_ROOT = 'esg_esacci'

    def __init__(self):
        super().__init__()
        self.env = Environment(loader=PackageLoader("cci_publisher", "templates"))
        self.env.trim_blocks = True
        self.env.lstrip_blocks = True

    def create_dataset(self, result, file_services):
        """
        Not used?

        :param result:
        :param file_services:
        :return:
        """
        this_id = result['name']

        # Going from [1:] to remove the first slash
        url_path = os.path.join(self.DS_ROOT, result['directory'][1:], this_id)
        a_meths = [AccessMethod(s, url_path, "NetCDF-4") for s in file_services]

        size = result['size']

        dataset = Dataset(id=this_id, access_methods=a_meths, size=size)

        return dataset

    def dataset_catalog(self, ds_id, opendap=False):
        """
        Build a THREDDS catalog and return the XML as a string

        :param ds_id: DRS ID
        :type ds_id: str

        :param opendap: Whether or not the service is available via opendap
        :type opendap: bool

        :return: XML string
        :rtype: string
        """
        # Work out which services are required
        file_services = {AvailableServices.HTTP.value}
        aggregation = None

        if opendap:
            file_services.add(AvailableServices.OPENDAP.value)

        all_services = file_services.copy()

        context = {
            "services": all_services,
            "dataset_id": ds_id,
            "aggregation": aggregation,
        }

        return self.render("dataset_catalog.xml", **context)

    def root_catalog(self, cat_paths, root_dir, name="THREDDS catalog"):
        """
        Build a root-level catalog that links to other catalogs, and return the
        XML as a string

        :param cat_paths: paths to dataset xml records
        :type cat_paths: list

        :param root_dir: the location of the root catalog.xml
        :type root_dir: str

        :param name: name of root catalog
        :type name: str

        :return: XML String
        :rtype: str
        """
        catalogs = []

        for path in cat_paths:
            cat_name = get_catalog_name(path)

            # href must be relative to the root catalog itself
            href = os.path.relpath(path, start=root_dir)
            catalogs.append(CatalogRef(name=cat_name, title=cat_name,
                                       href=href))

        return self.render("root_catalog.xml", name=name, catalogs=catalogs)