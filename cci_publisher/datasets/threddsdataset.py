# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '25 Feb 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

import os
import sys
from collections import namedtuple
import xml.etree.cElementTree as ET
from cached_property import cached_property
from cci_publisher.aggregation.base import CCIAggregationCreator
from cci_publisher.aggregation.aerosol import CCIAerosolAggregationCreator
from tds_utils.partition_files import partition_files
from tds_utils.aggregation import AggregationError, CoordinatesError


def get_thredds_url(host, in_file):
    """
    Return the URL for a THREDDS catalog on a remote server
    """
    # TODO: get this from a command line argument (so it can be passed from
    # get_catalogs.py) instead of from the filename?
    head, fname = os.path.split(in_file)
    if fname.endswith(".xml"):
        fname = fname[:-4]
    numbered_subdir = os.path.basename(head)
    try:
        int(numbered_subdir)
    except ValueError:
        raise ValueError(
            "Could not get THREDDS URL from filename '{}'".format(in_file)
        )
    path = "{}/{}".format(numbered_subdir, fname)
    return "https://{host}/thredds/esacci/{path}.html".format(host=host, path=path)


class AggregationInfo(namedtuple("AggregationInfo", ["xml_element", "basename",
                                                     "sub_dir"])):
    """
    namedtuple to store information about an NcML aggregation
    - xml_element - instance of ThreddsXMLBase for the NcML document
    - basename    - basename of the to-be-created NcML file
    - sub_dir     - subdirectory of the root aggregations dir in which the
                    NcML file should be created
    """


class ThreddsXMLBase(object):
    """
    Base class re generic stuff we want to do to THREDDS XML files
    """

    def __init__(self,
                 ns="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0",
                 encoding="UTF-8",
                 xlink="http://www.w3.org/1999/xlink"):
        self.ns = ns
        self.encoding = encoding
        self.xlink = xlink
        self.in_filename = None
        self.tree = None
        self.root = None

    def set_root(self, root):
        self.tree = ET.ElementTree(root)
        self.root = root
        self.root.set("xmlns:xlink", self.xlink)

    def read(self, filename):
        self.in_filename = filename
        ET.register_namespace("", self.ns)
        self.tree = ET.ElementTree()
        self.tree.parse(filename)
        self.root = self.tree.getroot()
        self.root.set("xmlns:xlink", self.xlink)

    def write(self, filename):
        tmpfile = filename + ".tmp"
        self.tree.write(tmpfile, encoding=self.encoding, xml_declaration=True)
        os.system("xmllint --format %s > %s" % (tmpfile, filename))
        os.remove(tmpfile)

    def tag_full_name(self, tag_base_name):
        return "{%s}%s" % (self.ns, tag_base_name)

    def tag_base_name(self, tag_full_name):
        return tag_full_name[tag_full_name.index("}") + 1:]

    def tag_base_name_is(self, element, tag_name):
        return self.tag_base_name(element.tag) == tag_name

    def insert_element_before_similar(self, parent, new_child):
        """
        Add a child element, if possible putting it before another child with the same tag
        """
        new_tag = self.tag_base_name(new_child.tag)
        for i, child in enumerate(parent.getchildren()):
            if not self.tag_base_name_is(child, new_tag):
                parent.insert(i, new_child)
                break
        else:
            parent.append(new_child)

    def new_element(self, tag_base_name, *args, **attributes):
        """
        Create a new element. Arguments are the tag name, a single optional positional argument
        which is the element text, and then the attributes.
        """
        el = ET.Element(self.tag_full_name(tag_base_name), **attributes)
        if args:
            (text,) = args
            el.text = text
        return el

    def new_child(self, parent, *args, **kwargs):
        """
        As new_element, but add result as child of specified parent element
        """
        child = self.new_element(*args, **kwargs)
        parent.append(child)
        return child


class ThreddsXMLDataset(ThreddsXMLBase):
    """
    A class for processing THREDDS XML files and tweaking them to add WMS tags
    and NcML aggregation
    """

    def __init__(self, aggregations_dir, thredds_server,
                 do_wcs=False, netcdf_files=[], **kwargs):
        """
        aggregations_dir is the directory in which NcML files will be placed on the
        server (used to reference aggregations from the THREDDS catalog)
        """
        super().__init__(**kwargs)
        self.do_wcs = do_wcs
        self.aggregations_dir = aggregations_dir
        self.thredds_server = thredds_server
        self.aggregation = None
        self.netcdf_files = netcdf_files

    @cached_property
    def top_level_dataset(self):
        for child in self.root.getchildren():
            if self.tag_base_name_is(child, "dataset"):
                return child

    @cached_property
    def second_level_datasets(self):
        return [child for child in self.top_level_dataset.getchildren()
                if self.tag_base_name_is(child, "dataset")]

    @cached_property
    def dataset_id(self):
        return self.top_level_dataset.attrib["ID"]

    def insert_metadata(self):
        mt = self.new_element("metadata", inherited="true")
        self.new_child(mt, "serviceName", "all")
        self.new_child(mt, "authority", "pml.ac.uk:")
        self.new_child(mt, "dataType", "Grid")
        self.insert_element_before_similar(self.top_level_dataset, mt)

    def insert_wms_viewer(self, ds):
        self.new_child(ds, "property", name="viewer",
                       value="http://jasmin.eofrom.space/?wms_url={WMS}"
                             "?service=WMS&version=1.3.0"
                             "&request=GetCapabilities,GISportal Viewer")

    def insert_wms_service(self,
                           base="/thredds/wms/"):
        """
        Add a new 'service' element.
        """
        sv = self.new_element("service",
                              name="wms",
                              serviceType="WMS",
                              base=base)
        self.root.insert(0, sv)

    def insert_wcs_service(self,
                           base="/thredds/wcs/"):
        """
        Add a new 'service' element.
        """
        sv = self.new_element("service",
                              name="wcs",
                              serviceType="WCS",
                              base=base)
        self.root.insert(0, sv)

    def write(self, filename, agg_dir):
        """
        Write this catalog to 'filename', and save the aggregation in 'agg_dir'
        """
        super().write(filename)

        if self.aggregation:
            agg = self.aggregation
            abs_subdir = os.path.join(agg_dir, agg.sub_dir)
            if not os.path.isdir(abs_subdir):
                os.makedirs(abs_subdir)

            agg.xml_element.write(os.path.join(abs_subdir, agg.basename))

    def strip_restrict_access(self):
        """
        remove restrictAccess from the top-level dataset tag
        """
        att_name = "restrictAccess"
        att_dict = self.top_level_dataset.attrib
        if att_name in att_dict:
            del att_dict[att_name]

    def get_aggregation_creator_cls(self, agg_dim):
        """
        Return a subclass of CCIAggregationCreator used to create the NcML
        aggregation
        """
        if 'AEROSOL' in self.dataset_id:
            creator = CCIAerosolAggregationCreator
        else:
            creator = CCIAggregationCreator

        return creator(agg_dim)

    def add_aggregation(self, add_wms=False):
        """
        Create an NcML aggregation from netCDF files in this dataset, and link
        to them in the catalog.

        The NcML document and related info is saved in self.aggregation
        """
        # Get directory to store aggregation in by splitting file name into
        # its facets and having a subdirectory for each component.
        components = os.path.basename(self.in_filename).split(".")
        if components:
            if components[0] == "esacci":
                components.pop(0)
            if components[-1] == "xml":
                components.pop(-1)
        sub_dir = os.path.join(*components)

        services = ["opendap"]
        if add_wms:
            services.append("wms")
            if self.do_wcs:
                services.append("wcs")

        print(f"Creating aggregation '{self.dataset_id}'")

        # If file list looks like it contains heterogeneous files then show a
        # warning
        groups = partition_files(self.netcdf_files)
        if len(groups) > 1:
            msg = (f"WARNING: File list for dataset '{self.dataset_id}' may contain "
                   f"heterogeneous files (found {len(groups)} potential groups)")
            print(msg, file=sys.stderr)

        agg_dim = "time"
        creator = self.get_aggregation_creator_cls(agg_dim)
        # Open the first file to see if aggregation dimension is also a
        # variable -- if so then its values can be cached in the ncml
        cache = True
        with creator.dataset_reader_cls(self.netcdf_files[0]) as reader:
            try:
                reader.get_coord_values(agg_dim)
            except CoordinatesError:
                cache = False
                print("WARNING: Skipping coordinate value caching: variable "
                      "'{}' could not be read in first file".format(agg_dim),
                      file=sys.stderr)

        # Construct URL to THREDDS catalog on remote server (even though the
        # catalog does not yet exist on the remote server!)
        try:
            thredds_url = get_thredds_url(self.thredds_server, self.in_filename)
        except ValueError:
            # Fall back to root of THREDDS server, not specific catalog
            thredds_url = self.thredds_server

        try:
            agg_element = creator.create_aggregation(self.dataset_id, thredds_url, self.netcdf_files, cache=cache)
        except AggregationError:
            print("WARNING: Failed to create aggregation", file=sys.stderr)
            return

        ds = self.new_element("dataset", name=self.dataset_id, ID=self.dataset_id, urlPath=self.dataset_id)

        for service_name in services:
            access = self.new_element("access", serviceName=service_name,
                                      urlPath=self.dataset_id)
            # Add 'access' to new dataset so that it has the required
            # endpoints in THREDDS
            ds.append(access)
            # Add 'access' to the top-level dataset so that the esgf
            # publisher picks up the WMS endpoints when publishing to Solr
            self.top_level_dataset.append(access)

        agg_xml = ThreddsXMLBase()
        agg_xml.set_root(agg_element)

        agg_basename = f"{self.dataset_id}.ncml"
        self.aggregation = AggregationInfo(xml_element=agg_xml,
                                           basename=agg_basename,
                                           sub_dir=sub_dir)

        # Create a 'netcdf' element in the catalog that points to the file containing the
        # aggregation
        agg_full_path = os.path.join(self.aggregations_dir, sub_dir, agg_basename)
        self.new_child(ds, "netcdf", location=agg_full_path,
                       xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2")

        if add_wms:
            self.insert_wms_viewer(ds)

        self.top_level_dataset.append(ds)

    def all_changes(self, create_aggs=False, add_wms=False):
        self.strip_restrict_access()
        self.insert_metadata()

        if create_aggs:
            self.add_aggregation(add_wms=add_wms)

        # Add WMS/WCS services
        if add_wms:
            self.insert_wms_service()
            if self.do_wcs:
                self.insert_wcs_service()
