import prometheus_client as pc
import influxdb
import configparser
from .scripts.archive_metrics import ArchiveMetrics
from .scripts.lotus_metrics import LotusMetrics
from .scripts.cloud_metrics import CloudMetrics
from .scripts.network_metrics import NetworkMetrics
from .scripts.storage_metrics import StorageMetrics
from .scripts.tape_metrics import TapeMetrics
from .scripts.users_metrics import UsersMetrics
from .scripts.power_metrics import PowerMetrics
from .scripts.utils import get_influxdb_client
import time
import os
import pandas as pd
import re


class MetricsView(object):
    def __init__(self, metrics_group, req_metrics_file=os.environ['JASMIN_METRICS_ROOT']+'metrics.ini'):
        self.collector = pc.CollectorRegistry()
        self.metrics_group = metrics_group
        self.lotus = LotusMetrics()
        self.storage = StorageMetrics()
        self.cloud = CloudMetrics()
        self.arch = ArchiveMetrics()
        self.users = UsersMetrics()
        self.gws_consortium = pd.read_csv(os.environ['JASMIN_METRICS_ROOT']+'gws_consortiums.csv', header = 0)
        self.req_metrics = self.parse_metrics_config(req_metrics_file)

        self.service_status_list = {}
        
        self.met_funcs = {}
        for m in self.req_metrics:
            if m.startswith('lotus_'): 
                m_func = eval('self.lotus.get_{}'.format(m))
            elif m.startswith('archive_'):
                m_func = eval('self.arch.get_{}'.format(m))
            elif m.startswith('storage_'):
                m_func = eval('self.storage.get_{}'.format(m))
            elif m.startswith('cloud_'):
                m_func = eval('self.cloud.get_{}'.format(m))
            elif m.startswith('users_'):
                m_func = eval('self.users.get_{}'.format(m))
            else:
                raise ValueError('Metric {} failed function assignment')
            self.met_funcs[m] = m_func
            if 'storage_gws' in m:
                gauge = pc.Gauge(m, m, ['gws_name','consortium', 'volume_type'], registry=self.collector)
            elif 'cloud_tenancy' in m:
                gauge = pc.Gauge(m, m, ['type', 'tenancy'], registry=self.collector)
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
                    consortium = "other"
                    gws_name = gws['VolumeName'].split('/')[-1]
                    for index, line in self.gws_consortium.iterrows():
                        gws_name_match = re.sub('_vol\d', '', gws_name)
                        if 'wiser' in gws_name_match:
                            gws_name_match = 'wiser'
                        if line['gws_name'] == gws_name_match:
                            consortium = line['consortium']
                    self.service_status_list[m].labels(gws_name=gws_name, consortium=consortium, volume_type=gws['VolumeType']).set(self.met_funcs[m](gws['VolumeName']))

            elif 'cloud_tenancy' in m:
                for index, t in self.cloud.ten_df.iterrows():
                    tenancy = t['Project_Name']
                    if tenancy.endswith('-U'):
                       type_ = 'external'
                    elif tenancy.endswith('-M'):
                        type_ = 'managed'
                    elif tenancy.endswith('-S'):
                       type_ = 'special'
                    else:
                        type_ = 'unknown' 
                    self.service_status_list[m].labels(tenancy=tenancy, type=type_).set(self.met_funcs[m](t['Project_Name']))
            else:
                self.service_status_list[m].set(self.met_funcs[m]())

        return pc.generate_latest(registry=self.collector)

