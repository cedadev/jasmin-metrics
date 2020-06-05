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
        self.tape = TapeMetrics()
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
            elif m.startswith('tape_'):
                m_func = eval('self.tape.get_{}'.format(m))
            else:
                raise ValueError('Metric {} failed function assignment')
            self.met_funcs[m] = m_func
            if m.startswith('storage_gws'):
                gauge = pc.Gauge(m, m, ['gws_name','consortium', 'volume_type'], registry=self.collector)
            elif m.startswith('cloud_tenancy'):
                gauge = pc.Gauge(m, m, ['type', 'tenancy'], registry=self.collector)
            elif m.startswith('users_gws'):
                gauge = pc.Gauge(m, m, ['gws_name'], registry=self.collector)
            elif m.startswith('users_cloud'):
                gauge = pc.Gauge(m, m, ['type', 'tenancy'], registry=self.collector)
            elif m.startswith('users_vm'):
                gauge = pc.Gauge(m, m, ['vm_name', 'type'], registry=self.collector)
            elif m == 'users_jasmin_country':
                gauge = pc.Gauge(m, m, ['country'], registry=self.collector)
            elif m == 'users_jasmin_institution':
                gauge = pc.Gauge(m, m, ['institution'], registry=self.collector)
            elif m == 'users_jasmin_discipline':
                gauge = pc.Gauge(m, m, ['discipline'], registry=self.collector)
            elif m.startswith('tape_gws'):
                gauge = pc.Gauge(m, m, ['gws_name'], registry=self.collector)

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
            print('Working on {}'.format(m))
            if m.startswith('storage_gws'):

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

            elif m.startswith('cloud_tenancy'):
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

            elif m.startswith('users_gws'):
                for name in self.users.get_list_gws():
                    self.service_status_list[m].labels(gws_name=name).set(self.met_funcs[m](name))

            elif m.startswith('users_cloud'):
                for tenancy in self.users.get_list_tenancies():
                    if tenancy.endswith('-U'):
                        type_ = 'external'
                    elif tenancy.endswith('-M'):
                        type_ = 'managed'
                    elif tenancy.endswith('-S'):
                        type_ = 'special'
                    else:
                        type_ = 'unknown'
                    self.service_status_list[m].labels(tenancy=tenancy, type=type_).set(self.met_funcs[m](tenancy))

            elif m.startswith('users_vm'):
                for name in self.users.get_list_vms_project():
                    self.service_status_list[m].labels(vm_name=name, type='project').set(self.met_funcs[m](name))

            elif m == 'users_jasmin_institution':
                for i in self.users.get_list_institution():
                    self.service_status_list[m].labels(institution=i.name).set(self.met_funcs[m](i))

            elif m == 'users_jasmin_discipline':
                for d in self.users.get_list_disciplines():
                    self.service_status_list[m].labels(discipline=d).set(self.met_funcs[m](d))

            elif m == 'users_jasmin_country':
                for c in self.users.get_list_countries():
                    print('Working on {}'.format(c))
                    self.service_status_list[m].labels(country=c).set(self.met_funcs[m](c))

            elif m.startswith('tape_gws'):
                for g in self.tape.get_gws_list():
                    self.service_status_list[m].labels(gws_name=g).set(self.met_funcs[m](g))

            else:
                self.service_status_list[m].set(self.met_funcs[m]())

        return pc.generate_latest(registry=self.collector)

