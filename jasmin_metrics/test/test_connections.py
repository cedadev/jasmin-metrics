# Test the conections to the various sources that the metrics server relies on

import unittest
from ..scripts.xdmod import XdMOD
from ..scripts.utils import get_influxdb_client, get_host_metrics_report

class TestConnections(unittest.TestCase):
    def setup(self):
        pass

    def teardown(self):
        pass 

    def test_influxdb_conn(self):
        client = get_influxdb_client()
        self.assertTrue(client.get_list_measurements())

    def test_xdmod_conn(self):
        xd = XdMOD()
        data = xd.get_tsparam('total_cpu_hours', start_date='2020-03-03', end_date='2020-03-03')
        self.assertEqual(len(data), 1)

    def test_ganglia_conn(self):
        data = get_host_metrics_report('host401.jc.rl.ac.uk','mem_report')
        self.assertTrue(data)
