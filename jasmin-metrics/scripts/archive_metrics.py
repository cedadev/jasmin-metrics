import scripts.utils as ut
import logging
import time
import subprocess

class ArchiveMetrics:
    def __init__(self):
        pass
    
    def get_host_load(self, host, metric):
        # get the last value for the given host and load metric from ganglia
        data = ut.get_host_metrics_report(host,"load_report",period='hour')
        if metric == "load_one":
            data = data[0]
        last = data['datapoints'][-2][0] # sometimes the -1 element is zero so just take the -2
        return last

    def get_host_mem(self, host, metric):
        # get the last value for given host an mem metric from ganglia
        data = ut.get_host_metrics_report(host,"mem_report",period='hour')
        if metric == "swap":
            data = data[4]
        last = data['datapoints'][-2][0]
        return last

    def host_up(self, host):
        ping = subprocess.call(['ping', '-c', '1', host]) == 0

        if ping:
            ret_val = 1
        else:
            ret_val = 0

        return ret_val

    def get_archive_ingest1_up(self):
        return self.host_up('ingest1.ceda.ac.uk')

    def get_archive_ingest2_up(self):
        return self.host_up('ingest2.ceda.ac.uk')

    def get_archive_ingest3_up(self):
        return self.host_up('ingest3.ceda.ac.uk')

    def get_archive_ingest4_up(self):
        return self.host_up('ingest4.ceda.ac.uk')

    def get_archive_ingest5_up(self):
        return self.host_up('ingest5.ceda.ac.uk')

    def get_archive_rbq_up(self):
        return self.host_up('rbq.ceda.ac.uk')

    def get_archive_deposit1_up(self):
        return self.host_up('deposit1.ceda.ac.uk')

    def get_archive_deposit2_up(self):
        return self.host_up('deposit2.ceda.ac.uk')

    def get_archive_deposit3_up(self):
        return self.host_up('deposit3.ceda.ac.uk')

    def get_archive_deposit4_up(self):
        return self.host_up('deposit4.ceda.ac.uk')

    def get_archive_deposit5_up(self):
        return self.host_up('deposit5.ceda.ac.uk')

    def get_archive_ingest1_load_1min(self):
        return self.get_host_load('ingest1.ceda.ac.uk', "load_one")

    def get_archive_ingest1_mem_swap(self):
        return self.get_host_mem('ingest1.ceda.ac.uk', 'swap')

    def get_archive_ingest2_load_1min(self):
        return self.get_host_load('ingest2.ceda.ac.uk', "load_one")

    def get_archive_ingest2_mem_swap(self):
        return self.get_host_mem('ingest2.ceda.ac.uk', 'swap')

    def get_archive_ingest3_load_1min(self):
        return self.get_host_load('ingest3.ceda.ac.uk', "load_one")

    def get_archive_ingest3_mem_swap(self):
        return self.get_host_mem('ingest3.ceda.ac.uk', 'swap')

    def get_archive_ingest4_load_1min(self):
        return self.get_host_load('ingest4.ceda.ac.uk', "load_one")

    def get_archive_ingest4_mem_swap(self):
        return self.get_host_mem('ingest4.ceda.ac.uk', 'swap')

    def get_archive_ingest5_load_1min(self):
        return self.get_host_load('ingest5.ceda.ac.uk', "load_one")

    def get_archive_ingest5_mem_swap(self):
        return self.get_host_mem('ingest5.ceda.ac.uk', 'swap')

    def get_archive_deposit1_load_1min(self):
        return self.get_host_load('deposit1.ceda.ac.uk', "load_one")

    def get_archive_deposit1_mem_swap(self):
        return self.get_host_mem('deposit1.ceda.ac.uk', 'swap')

    def get_archive_deposit2_load_1min(self):
        return self.get_host_load('deposit2.ceda.ac.uk', "load_one")

    def get_archive_deposit2_mem_swap(self):
        return self.get_host_mem('deposit2.ceda.ac.uk', 'swap')

    def get_archive_deposit3_load_1min(self):
        return self.get_host_load('deposit3.ceda.ac.uk', "load_one")

    def get_archive_deposit3_mem_swap(self):
        return self.get_host_mem('deposit3.ceda.ac.uk', 'swap')

    def get_archive_deposit4_load_1min(self):
        return self.get_host_load('deposit4.ceda.ac.uk', "load_one")

    def get_archive_deposit4_mem_swap(self):
        return self.get_host_mem('deposit4.ceda.ac.uk', 'swap')

    def get_archive_deposit5_load_1min(self):
        return self.get_host_load('deposit5.ceda.ac.uk', "load_one")

    def get_archive_deposit5_mem_swap(self):
        return self.get_host_mem('deposit5.ceda.ac.uk', 'swap')

    def get_archive_rbq_load_1min(self):
        return self.get_host_load('rbq.ceda.ac.uk', 'load_one')

    def get_archive_rbq_mem_swap(self):
        return self.get_host_mem('rbq.ceda.ac.uk', 'swap')
