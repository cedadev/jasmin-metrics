from random import random
from influxdb import InfluxDBClient
from pprint import pprint

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

def get_scd_last_st():
	
    creds = get_creds()
    
    client = InfluxDBClient(creds['server'],
                           creds['port'], 
                           creds['uname'],
                           creds['pword'],
                           'Metrics')

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
	
    pb_factor = 10**6
	
    total = {'PFS-cap': 0,
			 'PFS-com': 0,
			 'PFS-use': 0,
			 'QB-cap' : 0,
			 'QB-com' : 0,
			 'QB-use' : 0,
			 }			 
	
    for d in data:
        if 'PAN' in d:
            total['PFS-cap'] += data[d]['rawcap']/1.3/pb_factor
            total['PFS-com'] += data[d]['rawcom']/1.3/pb_factor
            total['PFS-use'] += data[d]['rawuse']/1.3/pb_factor
            
        elif 'QB' in d: 
            # worked out from mon dashboard
            total['QB-cap'] += data[d]['rawcap']/pb_factor
            total['QB-com'] += data[d]['rawuse']/pb_factor
    
    total['tot-cap'] = total['PFS-cap']+total['QB-cap']
    total['tot-com'] = total['PFS-com']+total['QB-com']
    total['tot-use'] = total['PFS-use']+total['QB-use']
   
    return total


def get_storage_total():           
    return get_storage_summary()['tot-cap']
            
def get_storage_used():    
    return get_storage_summary()['tot-com']
    
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
