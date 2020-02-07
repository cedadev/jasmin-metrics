import unittest
from scripts.lotus_metrics import LotusMetrics
from pprint import pprint


class TestLotusMetrics(unittest.TestCase):
    def test_checkall(self):
        pass

class CheckAll:
    def __init__(self):
        self.lotus = LotusMetrics()

    def run_all(self):
        pprint('Lotus core hours: {}'.format(self.lotus.get_lotus_core_hours()))
        pprint('Lotus avg core hours: {}'.format(self.lotus.get_lotus_core_hours_avg()))
        pprint('Lotus util: {}'.format(self.lotus.get_lotus_util()))
        pprint('Lotus job procs min: {}'.format(self.lotus.get_lotus_job_proc_min()))
        pprint('Lotus job procs avg: {}'.format(self.lotus.get_lotus_job_proc_avg()))
        pprint('Lotus job procs max: {}'.format(self.lotus.get_lotus_job_proc_max()))
        pprint('Lotus job finished: {}'.format(self.lotus.get_lotus_job_count_finished()))
        pprint('Lotus job running: {}'.format(self.lotus.get_lotus_job_count_running()))
        pprint('Lotus job started: {}'.format(self.lotus.get_lotus_job_count_started()))
        pprint('Lotus job submitted: {}'.format(self.lotus.get_lotus_job_count_submitted()))

if __name__ == '__main__':
    #unittest.main()
    run_check = CheckAll()
    run_check.run_all()