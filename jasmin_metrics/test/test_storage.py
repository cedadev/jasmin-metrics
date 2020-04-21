import unittest
from pandas import DataFrame
from ..scripts.storage_metrics import StorageMetrics

class TestMetrics(unittest.TestCase):
    def setUp(self):
        self.sm = StorageMetrics()

    def teardown(self):
        pass

    def test_gws_df(self):
        #print(type(self.sm.gws_df))
        self.assertEqual(type(self.sm.gws_df), DataFrame)

    def test_gws_provision(self):
        gws_sample = self.sm.gws_df['VolumeName'][0]
        self.assertEqual(float, type(self.sm.get_storage_gws_provision(gws_sample)))
    def test_gws_used(self):
        gws_sample = self.sm.gws_df['VolumeName'][0]
        self.assertEqual(float, type(self.sm.get_storage_gws_used(gws_sample)))
