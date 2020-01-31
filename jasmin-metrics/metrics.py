from random import random
from influxdb import InfluxDBClient
from pprint import pprint
import numpy as np
import re
import time
import requests
import scipy.integrate as it

# get the dist of storage totals from scd's influx
# the 'last' items gathered
def get_creds(loc='/root/creds.ini'):
    creds = {'sever' : '',
             'port' : '',
             'uname' : '',
             'pword' : ''}
             
    with open(loc,'r') as f:
        for line in f:
            stripped = line.strip().split(':')
            creds[stripped[0]] = stripped[1]
    return creds

def get_influxdb_client(db='Metrics'):
    creds = get_creds()
    
    client = InfluxDBClient(creds['server'],
                           creds['port'], 
                           creds['uname'],
                           creds['pword'],
                           db)
    return client

def get_scd_last_elt():
    """ Get the last storage from 'EquallogicTotal' measurements.
    """
    
    client = get_influxdb_client()

    last_res = client.query('select * from EquallogicTotals where time > now() - 1d')
    raw_data = last_res.raw['series']

    data = {}
    for d in raw_data[0]['values']:
        data['{}-{}'.format(d[-1],d[1])] = {
                        'time': d[0],
                        'rawcap': d[2],
                        'rawuse': d[4],
                        }
    return data 

def get_scd_last_st():
    """ Get the details of the last volume total usage from SCD's InfluxDB. From 'StorageTotals' measurements.
    """
    client = get_influxdb_client()

    	#last_res = client.query('select * from StorageTotals group by * order by desc limit 1')
    last_res = client.query('select * from StorageTotals WHERE time > now() - 1d')
	#print (last_res)# raw passes back a dict object
    raw_data = last_res.raw['series']
    
    data = {}
    for d in raw_data[0]['values']:
        if 'PAN' in d[-1]:
            data['{}-{}'.format(d[-1],d[1])] = {
                        'time': d[0],
                        'rawcap': d[5],
                        'rawcom': d[6],
                        'rawuse': d[7],
                        'logcap': d[2],
                        'logcom': d[3],
                        'loguse': d[4],
                        }
        else: 
            data['{}'.format(d[-1])] = {
                    'time': d[0],
                    'rawcap': d[5],
                    'rawcom': d[6],
                    'rawuse': d[7],
                    'logcap': d[2],
                    'logcom': d[3],
                    'loguse': d[4],
                    }
    return data


def get_storage_summary():
	# returns the total PFS available
	# Scaled by 1.3 to account for RAID6+
	
    data = get_scd_last_st()
	
    factor = 10**3
	
    total = {'PFS-cap': 0,
			 'PFS-com': 0,
			 'PFS-use': 0,
			 'QB-cap' : 0,
			 'QB-com' : 0,
			 'QB-use' : 0,
			 'EL-cap' : 0,
                         'EL-use' : 0 }			 
	
    for d in data:
        if 'PAN' in d:
            total['PFS-cap'] += data[d]['rawcap']/1.3/factor
            total['PFS-com'] += data[d]['rawcom']/1.3/factor
            total['PFS-use'] += data[d]['rawuse']/1.3/factor
            
        elif 'QB' in d: 
            # worked out from mon dashboard
            total['QB-cap'] += data[d]['rawcap']/factor
            total['QB-com'] += data[d]['rawuse']/factor
    
    data = get_scd_last_elt()
    for d in data:
            total['EL-cap'] += data[d]['rawcap']/factor
            total['EL-use'] += data[d]['rawuse']/factor
    
    total['tot-cap'] = total['PFS-cap']+total['QB-cap']+total['EL-cap']
    total['tot-com'] = total['PFS-com']+total['QB-com']+get_openstack_vms_storage_quota()
    total['tot-use'] = total['PFS-use']+total['QB-use']+total['EL-use']+get_openstack_vms_storage_used()
   
    return total

def get_lotus_host_data():
    client = get_influxdb_client('lsfMetrics')
    data = client.query('select * from lsfhoststatus where time > now() - 30m')
    return data

def get_lotus_hosts_count():
    client = get_influxdb_client('lsfMetrics')
    q = client.query('select hostname,maxjobs from lsfhoststatus where time > now() - 30m')
    if len(q) == 0:
        q = client.query('select hostname,maxjobs from lsfhoststatus where time > now() - 1h')
    hosts = [x[1] for x in q.raw['series'][0]['values'] if 'host' in x[1]]
    return len(hosts)

def get_lotus_cores_count():
    client = get_influxdb_client('lsfMetrics')
    q = client.query('select hostname,maxjobs from lsfhoststatus where time > now() - 30m')   
    if len(q) == 0:
        q =  client.query('select hostname,maxjobs from lsfhoststatus where time > now() - 1h')
    cores = [x[2] for x in q.raw['series'][0]['values'] if 'host' in x[1]]
    return np.sum(cores)

def get_lotus_mem_total():
    # Note that this only includes hosts where the memory is in the hostgroup
    client = get_influxdb_client('lsfMetrics')
    q = client.query('select hostgroup,hostname,memfreeGB from lsfhoststatus where time > now() - 30m')   
    if len(q) == 0:
        q =  client.query('select hostgroup,hostname,memfreeGB from lsfhoststatus where time > now() - 1h')
    hosts = q.raw['series'][0]['values']
    
    mem = 0
    for h in hosts:
        if 'host' in h[2]:
            try: 
                mem += int(re.findall(r'\d+', h[1])[0])
            except IndexError:
                mem += 0

    return mem
   
def get_openstack_vms_count():
    client = get_influxdb_client()
    q = client.query('select * from Openstack where time > now() - 1h') #seems to be a report every hour ish
    if len(q) == 0:
        q = client.query('select * from Openstack where time > now() - 2h')
    count = len([x[2] for x in q.raw['series'][0]['values']])
    return count

def get_openstack_vms_cpus_quota():
    client = get_influxdb_client()
    q = client.query('select VCPUs_Quota from Openstack where time > now() - 1h')
    if len(q) == 0:
        q = client.query('select VCPUs_Quota from Openstack where time > now() - 2h')

    tot = np.sum([x[1] for x in q.raw['series'][0]['values']])

    return tot

def get_openstack_vms_ram_quota():
    client = get_influxdb_client()
    q = client.query('select RAM_Quota_MiB from Openstack where time > now() - 1h')
    if len(q) == 0:
        q = client.query('select RAM_Quota_MiB from Openstack where time > now() - 2h')
    tot = np.sum([x[1] for x in q.raw['series'][0]['values']])
    return tot/10**3

def get_openstack_vms_storage_quota():
    client = get_influxdb_client()
    q = client.query('select Storage_Quota_GiB from Openstack where time > now() - 1h')
    if len(q) == 0:
        q = client.query('select Storage_Quota_GiB from Openstack where time > now() - 2h')
    tot = np.sum([x[1] for x in q.raw['series'][0]['values']])
    return tot/10**3

def get_openstack_vms_cpus_used():
    client = get_influxdb_client()
    q = client.query('select VCPUs_Used from Openstack where time > now() - 1h')
    if len(q) == 0:
        q = client.query('select VCPUs_Used from Openstack where time > now() - 2h')
    tot = np.sum([x[1] for x in q.raw['series'][0]['values']])
    return tot

def get_openstack_vms_ram_used():
    client = get_influxdb_client()
    q = client.query('select RAM_Used_MiB from Openstack where time > now() - 1h')

    if len(q) == 0:
        q = client.query('select RAM_Used_MiB from Openstack where time > now() - 2h')
    tot = np.sum([x[1] for x in q.raw['series'][0]['values']])
    return tot/10**3

def get_openstack_vms_storage_used():
    client = get_influxdb_client()
    q = client.query('select Volume_Storage_Used_GiB from Openstack where time > now() - 1h')
    if len(q) == 0:
        q = client.query('select Volume_Storage_Used_GiB from Openstack where time > now() - 2h')
    tot = np.sum([x[1] for x in q.raw['series'][0]['values']])
    return tot/10**3


def get_host_metrics_report(host,metric, period = '2month'):
    """ Get a json document which is the ganglia report"""
    
    url = "http://mgmt.jc.rl.ac.uk/ganglia/graph.php?r={}&h={}&m=load_one&s=by+name&mc=2&g={}&c=JASMIN+Cluster&json=1".format(period,host,metric)
    r = requests.get(url)
    raw_json = r.json()

    return raw_json

def get_all_lotus_hosts():
    client = get_influxdb_client('lsfMetrics')
    q = client.query('select hostname,maxjobs from lsfhoststatus where time > now() - 30m')
    if len(q) == 0:
        q = client.query('select hostname,maxjobs from lsfhoststatus where time > now() - 1h')
    hosts = [x[1] for x in q.raw['series'][0]['values'] if 'host' in x[1]]
    return hosts


def calc_dt(data):
    time = [x[1] for x in data['datapoints']]
    dt = time[1]-time[0]
    return dt


def get_lotus_network_traffic_tbmonth():

    # should be able to get the last 3 months from ganlia and integrate over it
   
    # can get the host names from influx and construct the ganlia path from it
    hosts = get_all_lotus_hosts()

    in_sum = 0
    out_sum = 0

    for h in hosts:
       #get the in first
       data = get_host_metrics_report(h,'network_report')
       in_report = data[0]
       dt = calc_dt(in_report) 
       in_data = [x[0] for x in in_report['datapoints']]  
       in_int = it.simps(in_data, dx=dt)    
       if in_report['metric_name'].strip()== 'In':
           # do in sum
           in_sum += in_int
       elif in_report['metric_name'].strip() == 'Out':
           # do out sum
           out_sum += in_int
       else:
           raise ValueError('Metric seems to be wrong')

       out_report = data[1]
       dt = calc_dt(out_report)
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
    tb_factor = 10**12
    month = 2 # divide by the period the intg is over 
    in_sum = in_sum/tb_factor/month
    out_sum = out_sum/tb_factor/month    

    return in_sum, out_sum
    
def get_lotus_network_traffic_now():
    hosts = get_all_lotus_hosts()

    in_sum = 0
    out_sum = 0

    for h in hosts:
       #get the in first
       data = get_host_metrics_report(h,'network_report', period='hour')
       in_report = data[0]
       in_val = np.sum([x[0] for x in in_report['datapoints']][-5:])/(5)
       if in_report['metric_name'].strip()== 'In':
           # do in sum
           in_sum += in_val
       elif in_report['metric_name'].strip() == 'Out':
           # do out sum
           out_sum += in_val
       else:
           raise ValueError('Metric seems to be wrong')

       out_report = data[1]
       out_val = np.sum([x[0] for x in out_report['datapoints']][-5:])/(5)
       if out_report['metric_name'].strip() == 'Out':
           # do out sum
           out_sum += out_val
       elif out_report['metric_name'].strip() == 'In':
           # do in sum
           in_sum += out_val
       else: 
           raise ValueError('Metrics seems to be wrong')
    
    # convert into GB
    size_factor = 10**9 
    in_sum = in_sum/size_factor
    out_sum = out_sum/size_factor    

    return in_sum, out_sum
  
def calc_lotus_core_hours():
    # calculates the number of cour hours over the previous month
    hosts = get_all_lotus_hosts()
    
    corehours = 0
    cpus = 0
    for h in hosts:
        data = get_host_metrics_report(h,'load_report', period='month')
        proc_data = data[2]
        cpu_data = data[1]
        dt = calc_dt(proc_data)
        proc_data = [x[0] for x in proc_data['datapoints']]
        tot = it.simps(proc_data, dx=dt)

        corehours += tot/3600
        
        cpu_data = [x[0] for x in cpu_data['datapoints']]
        totcpus = it.simps(cpu_data, dx=dt)

        cpus += totcpus/3600
    util = corehours/cpus*100

    return cpus, corehours, util

def get_lotus_core_hours():
    return calc_lotus_core_hours()[1]

def get_lotus_util():
    return calc_lotus_core_hours()[2]
 
def get_lotus_network_in():
    return get_lotus_network_traffic_now()[0]

def get_lotus_network_out():
    return get_lotus_network_traffic_now()[1] 

def get_lotus_tbmonth_in():
    return get_lotus_network_traffic_tbmonth()[0]

def get_lotus_tbmonth_out():
    return get_lotus_network_traffic_tbmonth()[1]

def get_storage_total():           
    return get_storage_summary()['tot-cap']
            
def get_storage_used():    
    return get_storage_summary()['tot-use']

def get_storage_com():
    return get_storage_summary()['tot-com']

def get_storage_el_total():
    return get_storage_summary()['EL-cap']

def get_storage_el_used():
    return get_storage_summary()['EL-use']
    
def get_storage_pfs_total():
    return get_storage_summary()['PFS-cap']
    
def get_storage_pfs_used():
    return get_storage_summary()['PFS-com']
    
def get_storage_sof_total():
    return get_storage_summary()['QB-cap']
    
def get_storage_sof_used():
    return get_storage_summary()['QB-com']
    
            

if __name__ == "__main__":
    pass
    #pprint(get_scd_last_st())
    #pprint(get_storage_total())
    pprint(get_lotus_util())

