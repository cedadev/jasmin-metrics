import unittest
from ..metrics import MetricsView

class TestMetrics(unittest.TestCase):
    def setup(self):
        pass

    def teardown(self):
        pass

    def test_daily_metrics_list(self):
        mv = MetricsView('daily')
        self.assertTrue(mv.req_metrics)

    def test_weekly_metrics_list(self):
        mv = MetricsView('weekly')
        self.assertTrue(mv.req_metrics)

    def test_monthly_metrics_list(self):
        mv = MetricsView('monthly')
        self.assertTrue(mv.req_metrics)

    def test_freq_metrics_list(self):
        mv = MetricsView('freq')
        self.assertTrue(mv.req_metrics)

    def test_archive_metrics_list(self):
        mv = MetricsView('archive')
        self.assertTrue(mv.req_metrics)

