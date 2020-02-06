import scripts.utils as ut
import requests
import scipy.integrate as it
import numpy as np
import re

class lotus_metrics:

    def __init__(self):
        self.client = ut.get_influxdb_client('lsfMetrics')
        self.hosts = self.get_all_lotus_hosts()

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


    def calc_lotus_core_hours(self):
        # calculates the number of cour hours over the previous month

        corehours = 0
        cpus = 0
        for h in self.hosts:
            data = self.get_host_metrics_report(h, 'load_report', period='month')
            proc_data = data[2]
            cpu_data = data[1]
            dt = self.calc_dt(proc_data)
            proc_data = [x[0] for x in proc_data['datapoints']]
            tot = it.simps(proc_data, dx=dt)

            corehours += tot / 3600

            cpu_data = [x[0] for x in cpu_data['datapoints']]
            totcpus = it.simps(cpu_data, dx=dt)

            cpus += totcpus / 3600
        util = corehours / cpus * 100

        return cpus, corehours, util


    def get_lotus_core_hours(self):
        return self.calc_lotus_core_hours()[1]


    def get_lotus_util(self):
        return self.calc_lotus_core_hours()[2]


    def get_lotus_network_in(self):
        return self.get_lotus_network_traffic_now()[0]


    def get_lotus_network_out(self):
        return self.get_lotus_network_traffic_now()[1]


    def get_lotus_tbmonth_in(self):
        return self.get_lotus_network_traffic_tbmonth()[0]


    def get_lotus_tbmonth_out(self):
        return self.get_lotus_network_traffic_tbmonth()[1]
