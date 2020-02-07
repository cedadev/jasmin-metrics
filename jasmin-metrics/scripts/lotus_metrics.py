import scripts.utils as ut
import requests
import scipy.integrate as it
import numpy as np
import re
from scripts.xdmod import XdMOD
import datetime

class LotusMetrics:

    def __init__(self):
        #self.client = ut.get_influxdb_client('lsfMetrics')
        #self.hosts = self.get_all_lotus_hosts()
        self.xdmod = XdMOD()
        self.today = datetime.datetime.now().strftime('%Y-%m-%d')
        self.yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        self.min3day = (datetime.datetime.now() - datetime.timedelta(days=3)).strftime('%Y-%m-%d')
        self.minweek = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
        self.minmonth = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')


    def get_lotus_host_data(self):

        data = self.client.query('select * from lsfhoststatus where time > now() - 30m')
        return data

    def get_lotus_hosts_count(self):
        q = self.client.query('select hostname,maxjobs from lsfhoststatus where time > now() - 30m')
        if len(q) == 0:
            q = self.client.query('select hostname,maxjobs from lsfhoststatus where time > now() - 1h')
        hosts = [x[1] for x in q.raw['series'][0]['values'] if 'host' in x[1]]
        return len(hosts)

    def get_lotus_cores_count(self):
        q = self.client.query('select hostname,maxjobs from lsfhoststatus where time > now() - 30m')
        if len(q) == 0:
            q =  self.client.query('select hostname,maxjobs from lsfhoststatus where time > now() - 1h')
        cores = [x[2] for x in q.raw['series'][0]['values'] if 'host' in x[1]]
        return np.sum(cores)

    def get_lotus_mem_total(self):
        # Note that this only includes hosts where the memory is in the hostgroup
        q = self.client.query('select hostgroup,hostname,memfreeGB from lsfhoststatus where time > now() - 30m')
        if len(q) == 0:
            q =  self.client.query('select hostgroup,hostname,memfreeGB from lsfhoststatus where time > now() - 1h')
        hosts = q.raw['series'][0]['values']

        mem = 0
        for h in hosts:
            if 'host' in h[2]:
                try:
                    mem += int(re.findall(r'\d+', h[1])[0])
                except IndexError:
                    mem += 0

        return mem


    def get_host_metrics_report(self, host, metric, period='2month'):
        """ Get a json document which is the ganglia report"""

        url = "http://mgmt.jc.rl.ac.uk/ganglia/graph.php?r={}&h={}&m=load_one&s=by+name&mc=2&g={}&c=JASMIN+Cluster&json=1".format(
            period, host, metric)
        r = requests.get(url)
        raw_json = r.json()

        return raw_json


    def get_all_lotus_hosts(self):
        q = self.client.query('select hostname,maxjobs from lsfhoststatus where time > now() - 30m')
        if len(q) == 0:
            q = self.client.query('select hostname,maxjobs from lsfhoststatus where time > now() - 1h')
        hosts = [x[1] for x in q.raw['series'][0]['values'] if 'host' in x[1]]
        return hosts


    def calc_dt(self, data):
        time = [x[1] for x in data['datapoints']]
        dt = time[1] - time[0]
        return dt


    def get_lotus_network_traffic_tbmonth(self):
        # should be able to get the last 3 months from ganlia and integrate over it

        in_sum = 0
        out_sum = 0

        for h in self.hosts:
            # get the in first
            data = self.get_host_metrics_report(h, 'network_report')
            in_report = data[0]
            dt = self.calc_dt(in_report)
            in_data = [x[0] for x in in_report['datapoints']]
            in_int = it.simps(in_data, dx=dt)
            if in_report['metric_name'].strip() == 'In':
                # do in sum
                in_sum += in_int
            elif in_report['metric_name'].strip() == 'Out':
                # do out sum
                out_sum += in_int
            else:
                raise ValueError('Metric seems to be wrong')

            out_report = data[1]
            dt = self.calc_dt(out_report)
            out_data = [x[0] for x in out_report['datapoints']]
            out_int = it.simps(out_data, dx=dt)
            if out_report['metric_name'].strip() == 'Out':
                # do out sum
                out_sum += out_int
            elif out_report['metric_name'].strip() == 'In':
                # do in sum
                in_sum += out_int
            else:
                raise ValueError('Metrics seems to be wrong')

        # convert into TB/month
        tb_factor = 10 ** 12
        month = 2  # divide by the period the intg is over
        in_sum = in_sum / tb_factor / month
        out_sum = out_sum / tb_factor / month

        return in_sum, out_sum


    def get_lotus_network_traffic_now(self):

        in_sum = 0
        out_sum = 0

        for h in self.hosts:
            # get the in first
            data = self.get_host_metrics_report(h, 'network_report', period='hour')
            in_report = data[0]
            in_val = np.sum([x[0] for x in in_report['datapoints']][-5:]) / (5)
            if in_report['metric_name'].strip() == 'In':
                # do in sum
                in_sum += in_val
            elif in_report['metric_name'].strip() == 'Out':
                # do out sum
                out_sum += in_val
            else:
                raise ValueError('Metric seems to be wrong')

            out_report = data[1]
            out_val = np.sum([x[0] for x in out_report['datapoints']][-5:]) / (5)
            if out_report['metric_name'].strip() == 'Out':
                # do out sum
                out_sum += out_val
            elif out_report['metric_name'].strip() == 'In':
                # do in sum
                in_sum += out_val
            else:
                raise ValueError('Metrics seems to be wrong')

        # convert into GB
        size_factor = 10 ** 9
        in_sum = in_sum / size_factor
        out_sum = out_sum / size_factor

        return in_sum, out_sum


    def lotus_core_hours(self,start,stop):

        array_core_hours = self.xdmod.get_tsparam('total_cpu_hours',start,stop)
        return array_core_hours

    def lotus_core_hours_avg(self,start,stop):
        array_core_hours = self.xdmod.get_tsparam('avg_cpu_hours', start,stop)
        return array_core_hours


    def lotus_util(self,start,stop):
        array_util = self.xdmod.get_tsparam('utilization', start, stop)
        return array_util

    def lotus_job_proc_min(self,start,stop):
        array_procs = self.xdmod.get_tsparam('min_processors',start,stop)
        return array_procs

    def lotus_job_proc_avg(self,start,stop):
        array_procs = self.xdmod.get_tsparam('avg_processors',start,stop)
        return array_procs

    def lotus_job_proc_max(self,start,stop):
        array_procs = self.xdmod.get_tsparam('max_processors',start,stop)
        return array_procs

    def lotus_job_count_finished(self,start,stop):
        array_count = self.xdmod.get_tsparam('job_count',start,stop)
        return array_count

    def lotus_job_count_started(self,start,stop):
        array_count = self.xdmod.get_tsparam('started_job_count',start,stop)
        return array_count

    def lotus_job_count_running(self,start,stop):
        array_count = self.xdmod.get_tsparam('running_job_count',start,stop)
        return array_count

    def lotus_job_count_submitted(self,start,stop):
        array_count = self.xdmod.get_tsparam('submitted_job_count',start,stop)
        return array_count

    def lotus_expansion_factor(self,start,stop):
        array_expf = self.xdmod.get_tsparam('expansion_factor',start,stop)
        return array_expf

    def lotus_wait_dur_avg(self,start,stop):
        array_wait = self.xdmod.get_tsparam('avg_waitduration_hours',start,stop)
        return array_wait

    def lotus_wait_dur_tot(self,start,stop):
        array_wait = self.xdmod.get_tsparam('total_waitduration_hours', start,stop)
        return array_wait

    def lotus_wall_dur_avg(self,start,stop):
        array_wall = self.xdmod.get_tsparam('avg_wallduration_hours',start,stop)
        return array_wall

    def lotus_wall_dur_tot(self,start,stop):
        array_wall = self.xdmod.get_tsparam('total_wallduration_hours', start,stop)
        return array_wall

    def get_lotus_core_hours_day(self):
        # get the last element from the xdmod extraction from today's data
        data = self.lotus_core_hours(self.yesterday, self.today)['total_cpu_hours']
        return data.sum()

    def get_lotus_core_hours_3day(self):
        # get the last element from the xdmod extraction from today's data
        data = self.lotus_core_hours(self.min3day, self.today)['total_cpu_hours']
        return data.sum()

    def get_lotus_core_hours_week(self):
        # get the last element from the xdmod extraction from today's data
        data = self.lotus_core_hours(self.minweek, self.today)['total_cpu_hours']
        return data.sum()


    def get_lotus_core_hours_month(self):
        # get the last element from the xdmod extraction from today's data
        data = self.lotus_core_hours(self.minmonth, self.today)['total_cpu_hours']
        return data.sum()


    def get_lotus_core_hours_avg_day(self):
        data = self.lotus_core_hours_avg(self.yesterday, self.today)
        return data.mean().to_numpy()[0]


    def get_lotus_core_hours_avg_3day(self):
        data = self.lotus_core_hours_avg(self.min3day, self.today)
        return data.mean().to_numpy()[0]


    def get_lotus_core_hours_avg_week(self):
        data = self.lotus_core_hours_avg(self.minweek, self.today)
        return data.mean().to_numpy()[0]


    def get_lotus_core_hours_avg_month(self):
        data = self.lotus_core_hours_avg(self.minmonth, self.today)
        return data.mean().to_numpy()[0]


    def get_lotus_util_day(self):
        data = self.lotus_util(self.yesterday, self.today)
        return data.mean().to_numpy()[0]


    def get_lotus_util_3day(self):
        data = self.lotus_util(self.min3day, self.today)
        return data.mean().to_numpy()[0]


    def get_lotus_util_week(self):
        data = self.lotus_util(self.minweek, self.today)
        return data.mean().to_numpy()[0]


    def get_lotus_util_month(self):
        data = self.lotus_util(self.minmonth, self.today)
        return data.mean().to_numpy()[0]


    def get_lotus_job_proc_min_day(self):
        data = self.lotus_job_proc_min(self.yesterday,self.today)
        return data.min().to_numpy()[0]


    def get_lotus_job_proc_min_3day(self):
        data = self.lotus_job_proc_min(self.min3day,self.today)
        return data.min().to_numpy()[0]


    def get_lotus_job_proc_min_week(self):
        data = self.lotus_job_proc_min(self.minweek,self.today)
        return data.min().to_numpy()[0]


    def get_lotus_job_proc_min_month(self):
        data = self.lotus_job_proc_min(self.minmonth,self.today)
        return data.min().to_numpy()[0]


    def get_lotus_job_proc_avg_day(self):
        data = self.lotus_job_proc_avg(self.yesterday,self.today)
        return data.mean().to_numpy()[0]


    def get_lotus_job_proc_avg_3day(self):
        data = self.lotus_job_proc_avg(self.min3day,self.today)
        return data.mean().to_numpy()[0]


    def get_lotus_job_proc_avg_week(self):
        data = self.lotus_job_proc_avg(self.minweek,self.today)
        return data.mean().to_numpy()[0]


    def get_lotus_job_proc_avg_month(self):
        data = self.lotus_job_proc_avg(self.minmonth,self.today)
        return data.mean().to_numpy()[0]


    def get_lotus_job_proc_max_day(self):
        data = self.lotus_job_proc_max(self.yesterday,self.today)
        return data.max().to_numpy()[0]


    def get_lotus_job_proc_max_3day(self):
        data = self.lotus_job_proc_max(self.min3day,self.today)
        return data.max().to_numpy()[0]


    def get_lotus_job_proc_max_week(self):
        data = self.lotus_job_proc_max(self.minweek,self.today)
        return data.max().to_numpy()[0]


    def get_lotus_job_proc_max_month(self):
        data = self.lotus_job_proc_max(self.minmonth,self.today)
        return data.max().to_numpy()[0]


    def get_lotus_job_count_finished_day(self):
        data = self.lotus_job_count_finished(self.yesterday,self.today)
        return data.sum().to_numpy()[0]


    def get_lotus_job_count_finished_3day(self):
        data = self.lotus_job_count_finished(self.min3day,self.today)
        return data.sum().to_numpy()[0]


    def get_lotus_job_count_finished_week(self):
        data = self.lotus_job_count_finished(self.minweek,self.today)
        return data.sum().to_numpy()[0]


    def get_lotus_job_count_finished_month(self):
        data = self.lotus_job_count_finished(self.minmonth,self.today)
        return data.sum().to_numpy()[0]


    def get_lotus_job_count_started_day(self):
        data = self.lotus_job_count_started(self.yesterday,self.today)
        return data.sum().to_numpy()[0]


    def get_lotus_job_count_started_3day(self):
        data = self.lotus_job_count_started(self.min3day,self.today)
        return data.sum().to_numpy()[0]


    def get_lotus_job_count_started_week(self):
        data = self.lotus_job_count_started(self.minweek,self.today)
        return data.sum().to_numpy()[0]


    def get_lotus_job_count_started_month(self):
        data = self.lotus_job_count_started(self.minmonth,self.today)
        return data.sum().to_numpy()[0]


    def get_lotus_job_count_running_day(self):
        data = self.lotus_job_count_running(self.yesterday,self.today)
        return data.sum().to_numpy()[0]


    def get_lotus_job_count_running_3day(self):
        data = self.lotus_job_count_running(self.min3day,self.today)
        return data.sum().to_numpy()[0]


    def get_lotus_job_count_running_week(self):
        data = self.lotus_job_count_running(self.minweek,self.today)
        return data.sum().to_numpy()[0]


    def get_lotus_job_count_running_month(self):
        data = self.lotus_job_count_running(self.minmonth,self.today)
        return data.sum().to_numpy()[0]


    def get_lotus_job_count_submitted_day(self):
        data = self.lotus_job_count_submitted(self.yesterday,self.today)
        return data.sum().to_numpy()[0]


    def get_lotus_job_count_submitted_3day(self):
        data = self.lotus_job_count_submitted(self.min3day,self.today)
        return data.sum().to_numpy()[0]


    def get_lotus_job_count_submitted_week(self):
        data = self.lotus_job_count_submitted(self.minweek,self.today)
        return data.sum().to_numpy()[0]


    def get_lotus_job_count_submitted_month(self):
        data = self.lotus_job_count_submitted(self.minmonth,self.today)
        return data.sum().to_numpy()[0]


    def get_lotus_network_in(self):
        return self.get_lotus_network_traffic_now()[0]


    def get_lotus_network_out(self):
        return self.get_lotus_network_traffic_now()[1]


    def get_lotus_tbmonth_in(self):
        return self.get_lotus_network_traffic_tbmonth()[0]


    def get_lotus_tbmonth_out(self):
        return self.get_lotus_network_traffic_tbmonth()[1]

    def get_lotus_expansion_factor(self):
        data = self.lotus_expansion_factor(self.yesterday,self.today)
        return data.to_numpy()[-1]

    def get_lotus_wait_dur_avg(self):
        data = self.lotus_wait_dur_avg(self.yesterday,self.today)
        return data.to_numpy()[-1]

    def get_lotus_wait_dur_tot(self):
        data = self.lotus_wait_dur_tot(self.yesterday,self.today)
        return data.to_numpy()[-1]

    def get_lotus_wall_dur_avg(self):
        data = self.lotus_wall_dur_avg(self.yesterday,self.today)
        return data.to_numpy()[-1]

    def get_lotus_wall_dur_tot(self):
        data = self.lotus_wall_dur_tot(self.yesterday,self.today)
        return data.to_numpy()[-1]
