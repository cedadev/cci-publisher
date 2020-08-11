# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '05 May 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

import unittest
from cci_publisher.state_store.state_store import StateStore

TEST_INDEX = 'opensearch-aggregation-state-test'

class TestStateStore(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.store = StateStore(index=TEST_INDEX, http_auth=('',''))

    @classmethod
    def tearDownClass(cls):
        cls.store.session.indices.delete(index=TEST_INDEX)

    def test_add_row(self):
        self.store.add_row('a.b', 10, True, True)

        self.assertTrue(self.store.get_dataset('a.b'))

    def test_has_updated(self):

        has_updated = self.store.has_updated('a.b', 10, True, True)

        self.assertTrue(has_updated)

        self.store.add_row('a.b', 10, True, True)

        has_updated = self.store.has_updated('a.b', 10, True, True)

        self.assertFalse(has_updated)

        has_updated = self.store.has_updated('a.b', 12, True, False)

        self.assertTrue(has_updated)

    def test_update(self):
        self.store.update('a.b', 10, True, True)

        self.assertTrue(self.store.get_dataset('a.b'))

        self.store.update('a.b', 12, True, True)

        self.assertTrue(self.store.get_dataset('a.b'))

    def test_delete_row(self):

        self.store.add_row('a.b', 10, True, True)

        self.store.delete_row('a.b')

        self.assertFalse(self.store.get_dataset('a.b'))


if __name__ == '__main__':
    unittest.main()