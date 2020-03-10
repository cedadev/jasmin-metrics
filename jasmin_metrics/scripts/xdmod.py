import logging
import time
import requests
import re
import json
import sys
import numpy as np
import pandas as pd
from pprint import pprint

class XdMOD(object):
    """ Provides a clunky interface to XDMOD"""
    def __init__(self, host='https://lotus-stats.jc.rl.ac.uk',timeout=100):

        self.host = host
        self.timeout = timeout
        self.interface = 'controllers/user_interface.php'
        self.gubbins = {'aggregation_unit':'Auto',
                        'combine_type':'stack',
                        'controller_module':'user_interface',
                        'dataset_type':'timeseries',
                        'display_type':'line',
                        #'font_size':'3',
                        'group_by':'none',
                        #'height':'1030',
                        #'hide_tooltip':'false',
                        #'interactive_elements':'y',
                        #'legend_type':'bottom_center',
                        'limit':'10',
                        'log_scale':'n',
                        'offset':'0',
                        'operation':'get_charts',
                        'public_user':'true',
                        'query_group':'tg_usage',
                        #'realm':'Jobs',
                        'scale':'1',
                        #'show_aggregate_labels':'n',
                        #'show_error_bars':'n',
                        #'show_error_labels':'n',
                        #'show_guide_lines':'y',
                        #'show_title':'y',
                        #'show_trend_line':'n',
                        #'thumbnail':'n',
                        #'timeframe_label':'Previous month',
                        #'width':'1267',
                        'inline':'y',
                        'format':'csv'
                        }

        self.statistics = {}#'utilisation':'statistic_Jobs_none_utilization',
                           #'cpu_usage':'total_cpu_hours',
                           #'uniq_users':'statistic_Jobs_none_active_person_count'}

    def _get_raw(self, statistic='total_cpu_hours', start_date='2016-01-01', end_date='2016-12-31'):
        """ Return raw XDMOD output"""
        url = '%s/%s' % (self.host, self.interface)
        for key,val in [('statistic',statistic), ('start_date',start_date), ('end_date',end_date)]:
            self.gubbins[key] = val
        try:
            logging.captureWarnings(True)
            r = requests.get(url, params=self.gubbins, timeout=self.timeout, verify=False)
            response = r.text
            logging.captureWarnings(False)
            self.__check_response(response)
            return response
        except requests.RequestException as e:
            print(e)
            raise requests.RequestException(e)

    def __check_response(self,text):
        """ Parse the response to make sure it's successful, if not raise error"""
        bits = text.split(',')
        success = bits[0].split(':')[1]
        if not success == 'true':
            message = bits[1].split(':')[1]
            raise ValueError(message)

    def _suck_data(self,string):
        """ Suck data elements out of string"""
        rc = re.compile(r'(?<="data":)(.*?)(?=,"cursor":)')
        result = rc.search(string)
        if result:
            raw = result.group()
            jraw = json.loads(raw)
            nresult = []
            for d in jraw:
                nresult.append([d['x'],d['y']])
            return nresult
        return result

    def _raw2df(self, raw_data, parameter):
        """ Re-order the raw data from get_csv and load into a pandas dataframe"""
        r = self._suck_data(raw_data)
        if r:
            data = np.array(r)
            df = pd.DataFrame({'date':data[:,0], parameter:data[:,1]})
            df['date'] = pd.to_datetime(df['date'],unit='ms').dt.date
            df.set_index('date', inplace=True)
            return df
        else:
            return r

    def get_tsparam(self, parameter, start_date='2014-01-01', end_date='today'):
        """ Get a timeseries of information from XDMOD """
        if parameter in self.statistics:
            statistic = self.statistics[parameter]
        else:
            statistic = parameter

        if end_date == 'today':
            end_date = time.strftime('%Y-%m-%d')
        raw = self._get_raw(statistic, start_date=start_date, end_date=end_date)

        result = self._raw2df(raw, parameter)
        if parameter in self.statistics:
            result.rename({statistic:parameter},inplace=True)
        return result


if __name__ == "__main__":
    '''Available metrics:
avg_cpu_hours
total_cpu_hours
max_processors
min_processors
normalized_avg_processors
avg_processors
avg_job_size_weighted_by_cpu_hours
avg_node_hours
total_node_hours
job_count
running_job_count
started_job_count
submitted_job_count
active_pi_count
active_resource_count
active_person_count
utilization
expansion_factor
avg_waitduration_hours
total_waitduration_hours
avg_wallduration_hours
total_wallduration_hours
    '''

    met = sys.argv[1]
    xd = XdMOD()
    #data = xd._get_raw(statistic=met,start_date='2019-12-01')
    data = xd.get_tsparam(met, start_date="2020-03-03", end_date="2020-03-03")
    pprint(data)
