from influxdb import InfluxDBClient
import os

def get_creds(loc='environ'):
    if loc == 'environ':
        try:
            loc = os.environ['JASMIN_METRICS_CRED_LOC']
        except Exception as e:
            loc = '/root/creds.ini'

    creds = {'sever' : '',
             'port' : '',
             'uname' : '',
             'pword' : ''}

    with open(loc ,'r') as f:
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

def get_host_metrics_report(self, host, metric, period='2month'):
    """ Get a json document which is the ganglia report"""

    url = "http://mgmt.jc.rl.ac.uk/ganglia/graph.php?r={}&h={}&m=load_one&s=by+name&mc=2&g={}&c=JASMIN+Cluster&json=1".format(
        period, host, metric)
    r = requests.get(url)
    raw_json = r.json()

    return raw_json
