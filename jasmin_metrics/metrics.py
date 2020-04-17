import prometheus_client as pc
import influxdb
import configparser
from .scripts.archive_metrics import ArchiveMetrics
from .scripts.lotus_metrics import LotusMetrics
from .scripts.managed_cloud_metrics import MCMetrics
from .scripts.unmanaged_cloud_metrics import UMCMetrics
from .scripts.network_metrics import NetworkMetrics
from .scripts.storage_metrics import StorageMetrics
from .scripts.tape_metrics import TapeMetrics
#from scripts.users_metrics import UsersMetrics
from .scripts.power_metrics import PowerMetrics
from .scripts.utils import get_influxdb_client
import time
import os


class MetricsView(object):
    def __init__(self, metrics_group, req_metrics_file=os.environ['JASMIN_METRICS_CONFIG']):
        self.collector = pc.CollectorRegistry()
        self.metrics_group = metrics_group
        self.lotus = LotusMetrics()
        self.storage = StorageMetrics()
        self.mc = MCMetrics()
        self.arch = ArchiveMetrics()
        #users = UsersMetrics()

        self.req_metrics = self.parse_metrics_config(req_metrics_file)

        self.service_status_list = {}
        
        self.met_funcs = {}
        for m in self.req_metrics:
            if 'lotus_' in m: 
                m_func = eval('self.lotus.get_{}'.format(m))
            elif 'archive_' in m:
                m_func = eval('self.arch.get_{}'.format(m))
            elif 'storage_' in m:
                m_func = eval('self.storage.get_{}'.format(m))
            self.met_funcs[m] = m_func
            if 'storage_gws' in m:
                gauge = pc.Gauge(m, m, ['gws_name',], registry=self.collector)
            else:
                gauge = pc.Gauge(m, m, registry=self.collector)
            self.service_status_list[m] = (gauge)


    def parse_metrics_config(self, fname):
        config = configparser.ConfigParser()
        config.read(fname)
        req_metrics = config.get('Metrics',self.metrics_group).split('\n')

        return req_metrics

    def create_view(self):
        for m in self.req_metrics:
            if 'storage_gws' in m:
                for index, gws in self.storage.gws_df.iterrows():
                    gws_name = gws['VolumeName'].split('/')[-1]
                    self.service_status_list[m].labels(gws_name=gws_name).set(self.met_funcs[m](gws['VolumeName']))
            else:
                self.service_status_list[m].set(self.met_funcs[m]())

        return pc.generate_latest(registry=self.collector)

