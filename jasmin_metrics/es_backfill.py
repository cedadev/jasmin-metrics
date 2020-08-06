#from .scripts.power_metrics import PowerBackfill
import datetime as dt

import pandas as pd
from ceda_elasticsearch_tools.elasticsearch import CEDAElasticsearchClient
from elasticsearch.helpers import bulk

#from .scripts.archive_metrics import ArchiveBackfill
from .scripts.lotus_metrics import LotusBackfill
#from .scripts.cloud_metrics import CloudBackfill
#from .scripts.network_metrics import NetworkBackfill
from .scripts.storage_metrics import StorageBackfill
#from .scripts.tape_metrics import TapeBackfill
from .scripts.users_metrics import UsersBackfill

es = CEDAElasticsearchClient(headers={
    'x-api-key': '52fa65222a022f77ca6d58a2a719886e708921c2ef44703e80be0dc7b23d4bf8'
})

class hashabledict(dict):
    # trying this to make the dict hashable for the generator
  def __key(self):
    return tuple((k,self[k]) for k in sorted(self))
  def __hash__(self):
    return hash(self.__key())
  def __eq__(self, other):
    return self.__key() == other.__key()

class BackfillMetric(object):
    def __init__(self, start, end, metric, metric_group, req_metrics_file='/root/jasmin-metrics/jasmin_metrics/metrics.ini'):
        self.start = start
        self.end = end
        self.metric = metric
        self.metric_group = metric_group
        self.lotus = LotusBackfill()
        self.storage = StorageBackfill()
        #self.cloud = CloudBackfill()
        #self.arch = ArchiveBackfill()
        self.users = UsersBackfill()
        #self.tape = TapeBackfill()
        self.data = []

        if metric.startswith('lotus_'):
            m_func = eval('self.lotus.get_{}'.format(metric))
        elif metric.startswith('archive_'):
            m_func = eval('self.arch.get_{}'.format(metric))
        elif metric.startswith('storage_'):
            m_func = eval('self.storage.get_{}'.format(metric))
        elif metric.startswith('cloud_'):
            m_func = eval('self.cloud.get_{}'.format(metric))
        elif metric.startswith('users_'):
            m_func = eval('self.users.get_{}'.format(metric))
        elif metric.startswith('tape_'):
            m_func = eval('self.tape.get_{}'.format(metric))
        else:
            raise ValueError('Metric {} failed function assignment')
    
        self.m_func = m_func

    def _gendocs(self):
        """ Process document """
        return self.m_func(self.start, self.end)

    def save_docs(self):
        bulk(es, self._gendocs())

if __name__ == "__main__":
    bfm = BackfillMetric('2020-03-01', '2020-04-01',
                         'users_jasmin_login_active_today', 'daily',)
    print(bfm.gendocs())

