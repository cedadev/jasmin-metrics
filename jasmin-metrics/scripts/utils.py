from influxdb import InfluxDBClient

def get_creds(loc='/root/creds.ini'):
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


