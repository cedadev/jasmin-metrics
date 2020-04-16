from django.test import Client
import unittest

class TestSite(unittest.TestCase):
    def setUp(self):
        self.client = Client()

    def test_index(self):
        resp = self.client.get('/')
        self.assertEqual(resp.status_code,200)

    def test_prom(self):
        resp = self.client.get('/metrics/monthly/')
        self.assertEqual(resp.status_code, 200)
        resp = self.client.get('/metrics/daily/')
        self.assertEqual(resp.status_code, 200)
        resp = self.client.get('/metrics/weekly/')
        self.assertEqual(resp.status_code, 200)
        resp = self.client.get('/metrics/freq/')
        self.assertEqual(resp.status_code, 200)
        resp = self.client.get('/metrics/archive/')
        self.assertEqual(resp.status_code, 200)
