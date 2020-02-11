import scripts.utils as ut

class ArchiveMetrics:
    def __init__(self):
        pass

    def get_host_metric(self, host, metric):
        # get the last value for the given host and metric from ganglia
        data = ut.get_host_metric_report(host,metric,period='hour')
        return data[-1]

    def get_archive_ingest1_load_1min(self):
        return self.get_host_metric('ingest1.ceda.ac.uk', "1-min")
