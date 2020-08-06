from .utils import *
import pandas as pd
import time, datetime
import numpy as np
import re

class StorageGather(object):
    def __init__(self):
        self.client = get_influxdb_client()
        self.update_time = time.time()

    def check_last_frame_gen(self):
        # the generation of the pandas frames is in the init, so we need to check that these have been updated recently
        # enough
        now = time.time()
        if now-self.update_time > 7200:
            # generate pandas frame for all services and pks
            self.gws_df = self.get_gws_df()
            # update the update time
            self.update_time = time.time()

    def get_scd_last_elt(self, time_range, backup_time_range):
        """ Get the last storage from 'EquallogicTotal' measurements.
        """

        last_res = self.client.query("select * from EquallogicTotals where {}}".format(time_range)) #time > now() - 1d
        if len(last_res) == 0:
            last_res = self.client.query("select * from EquallogicTotals where {}}".format(backup_time_range)) # time > now() - 28h
        

        raw_data = last_res.raw['series']

        data = {}
        for d in raw_data[0]['values']:
            data['{}-{}'.format(d[-1], d[1])] = {
                'time': d[0],
                'rawcap': d[2],
                'rawuse': d[4],
            }
        return data


    def get_scd_last_st(self, time_range, backup_time_range=None):
        """ Get the details of the last volume total usage from SCD's InfluxDB. From 'StorageTotals' measurements.
        """

        last_res = self.client.query("select * from StorageTotals WHERE {}}".format(time_range))
        raw_data = last_res.raw['series']

        data = {}
        for d in raw_data[0]['values']:
            if 'PAN' in d[-1]:
                data['{}-{}'.format(d[-1], d[1])] = {
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

    def get_gws_df(self, time_range, backup_time_range=None):
        # get a pandas dataframe containing all the gws volumes
        last_res = self.client.query("select * from FileStorage WHERE {}".format(time_range)) 
        raw = last_res.raw['series'][0]
        cols = raw['columns']
        data = raw['values']
        data_cp = []
        vol_type = []
        for d in data:
            if 'group_workspace' in d[-2] or 'gws' in d[-2]:
                if 'QB' in d[1]:
                    vol_type.append('SOF')
                    d[2] = d[2]/1.024
                    d[-1] = d[-1]/1.024
                elif 'PAN' in d[1]:
                    vol_type.append('PFS')
                    d[2] = d[2]/1.3
                    d[-1] = d[-1]/1.3
                else:
                    vol_type.append('UNKW')
                
                data_cp.append(d)

        df = pd.DataFrame(data_cp,columns=cols)
        df['VolumeName_gws'] = df['VolumeName']
        df['VolumeType'] = vol_type
        df.replace({'VolumeName':{'gws_':''}}, regex=True, inplace=True)
        df.replace({'VolumeName':{'jasmin_':''}}, regex=True, inplace=True)

        
        return df

    def get_archive_df(self, time_range):
        # get a pandas dataframe containing all the gws volumes
        last_res = self.client.query('select * from FileStorage WHERE {}'.format(time_range))
        raw = last_res.raw['series'][0]
        cols = raw['columns']
        data = raw['values']
        data_cp = []
        for d in data:
            if 'archvol' in d[-2]:
                data_cp.append(d)
        df = pd.DataFrame(data_cp,columns=cols)

        return df


class StorageMetrics(StorageGather):
    def __init__(self):        
        super().__init__()
        self.gws_df = self.get_gws_df(time_range="time > now() - 1d")
    

    def get_storage_gws_used(self, gws):
        self.check_last_frame_gen()
        return float(self.gws_df[self.gws_df['VolumeName'] == gws]['VolumeUsageGB'].values[0])

    def get_storage_gws_provision(self, gws):
        self.check_last_frame_gen()
        return float(self.gws_df[self.gws_df['VolumeName'] == gws]['VolumeCapacityGB'].values[0])

    def get_storage_summary(self):
        # returns the total PFS available
        # Scaled by 1.3 to account for RAID6+

        data = self.get_scd_last_st(time_range="time > now() - 1d")

        factor = 10 ** 3

        total = {'PFS-cap': 0,
                 'PFS-com': 0,
                 'PFS-use': 0,
                 'QB-cap': 0,
                 'QB-com': 0,
                 'QB-use': 0,
                 'EL-cap': 0,
                 'EL-use': 0}

        for d in data:
            if 'PAN' in d:
                total['PFS-cap'] += data[d]['rawcap'] / 1.3 / factor
                total['PFS-com'] += data[d]['rawcom'] / 1.3 / factor
                total['PFS-use'] += data[d]['rawuse'] / 1.3 / factor

            elif 'QB' in d:
                # worked out from mon dashboard
                total['QB-cap'] += data[d]['rawcap'] / factor
                total['QB-com'] += data[d]['rawuse'] / factor

        data = self.get_scd_last_elt()
        for d in data:
            total['EL-cap'] += data[d]['rawcap'] / factor
            total['EL-use'] += data[d]['rawuse'] / factor

        total['tot-cap'] = total['PFS-cap'] + total['QB-cap'] #+ total['EL-cap']
        total['tot-com'] = total['PFS-com'] + total['QB-com'] #+ get_openstack_vms_storage_quota()
        total['tot-use'] = total['PFS-use'] + total['QB-com'] #+ total['EL-use'] + get_openstack_vms_storage_used()

        return total


    def get_storage_total(self):
        return self.get_storage_summary()['tot-cap']


    def get_storage_used(self):
        return self.get_storage_summary()['tot-use']


    def get_storage_com(self):
        return self.get_storage_summary()['tot-com']


    def get_storage_el_total(self):
        return self.get_storage_summary()['EL-cap']


    def get_storage_el_used(self):
        return self.get_storage_summary()['EL-use']


    def get_storage_pfs_total(self):
        return self.get_storage_summary()['PFS-cap']


    def get_storage_pfs_used(self):
        return self.get_storage_summary()['PFS-com']


    def get_storage_sof_total(self):
        return self.get_storage_summary()['QB-cap']


    def get_storage_sof_used(self):
        return self.get_storage_summary()['QB-com']

    def get_storage_count_gws(self):
        self.check_last_frame_gen()
        return len(self.gws_df)

    def get_storage_total_gws_provision(self):
        self.check_last_frame_gen()
        return np.sum(self.gws_df['VolumeCapacityGB'])

    def get_storage_total_gws_used(self):
        self.check_last_frame_gen()
        return np.sum(self.gws_df['VolumeUsageGB'])

    def get_storage_total_archive_provision(self):
        df = self.get_archive_df()
        return np.sum(df['VolumeCapacityGB'])

    def get_storage_total_archive_used(self):
        df= self.get_archive_df()
        return np.sum(df['VolumeUsageGB'])



class StorageBackfill(StorageGather):
    def __init__(self):
        super().__init__()
        self.gws_consortium = pd.read_csv(os.environ['JASMIN_METRICS_ROOT']+'gws_consortiums.csv', header = 0)

    def get_storage_gws_used(self, start, end):
        dates = gen_time_list(start, end)

        for d in dates:
            t_dt = datetime.datetime.strptime(d, '%Y-%m-%dT%H:%M:%SZ')
            d_end = datetime.datetime.strftime(t_dt+datetime.timedelta(days=1), '%Y-%m-%dT%H:%M:%SZ')
            time_range = "time >= '{}' AND time < '{}'".format(d,d_end)
            # Overwrite gws dataframe with current time period required
            self.gws_df = self.get_gws_df(time_range=time_range)

            for index, gws in self.gws_df.iterrows():
                consortium = "other"
                gws_name = gws['VolumeName'].split('/')[-1]
                gws_vol = gws['VolumeName'].split('/')[-1]
                if re.match( 'vol\d', gws_name):
                    gws_name = gws['VolumeName'].split('/')[-2]
                for index, line in self.gws_consortium.iterrows():
                    gws_name_match = re.sub('_vol\d', '', gws_name)
                if 'wiser' in gws_name_match:
                    gws_name_match = 'wiser'
                elif 'primavera' in gws_name_match:
                    gws_name_match = 'primavera'
                if line['gws_name'] == gws_name_match:
                    consortium = line['consortium']
                vol_type = gws['VolumeType']
                gws_name_to_pass = gws['VolumeName']

                yield {
                    "_index": "mjones-test2",
                    "_source": {
                        "@timestamp": d,
                        "prometheus": {
                            "metrics": {'storage_gws_used': float(self.gws_df[self.gws_df['VolumeName'] == gws_name_to_pass]['VolumeUsageGB'].values[0])
                                        },
                            "labels": {
                                "gws_vol": gws_vol, 
                                "gws_name": gws_name_match, 
                                "consortium": consortium, 
                                "volume_type": vol_type,
                                "metric_name": 'storage_gws_used',
                                "instance": "localhost:8091",
                                "job": "prometheus"
                            }
                        },
                        "event": {
                            "duration": 5425153946,
                            "dataset": "prometheus.collector",
                            "module": "prometheus"
                        },
                        "metricset": {
                            "period": 1200000,
                            "name": "collector"
                        },
                        "service": {
                            "address": "localhost:8091",
                            "type": "prometheus"
                        },
                        "ecs": {
                            "version": "1.4.0"
                        },
                        "host": {
                            "hostname": "metrics1.jasmin.ac.uk",
                            "architecture": "x86_64",
                            "name": "metrics1.jasmin.ac.uk",
                            "os": {
                                "platform": "centos",
                                "version": "7 (Core)",
                                "family": "redhat",
                                "name": "CentOS Linux",
                                "kernel": "3.10.0-1062.18.1.el7.x86_64",
                                "codename": "Core"
                            },
                            "containerized": False
                        },
                        "agent": {
                            "ephemeral_id": "6570074e-f1f3-4fa3-aae4-e57ff12c71e6",
                            "hostname": "metrics1.jasmin.ac.uk",
                            "id": "515c3f57-1204-4a41-b84e-43c08b206a74",
                            "version": "7.6.2",
                            "type": "metricbeat"
                        }
                    }
                }
        
    def get_storage_gws_provision(self, start, end):
        dates = gen_time_list(start, end)

        for d in dates:
            t_dt = datetime.datetime.strptime(d, '%Y-%m-%dT%H:%M:%SZ')
            d_end = datetime.datetime.strftime(t_dt+datetime.timedelta(days=1), '%Y-%m-%dT%H:%M:%SZ')
            time_range = "time >= '{}' AND time < '{}'".format(d,d_end)
            # Overwrite gws dataframe with current time period required
            self.gws_df = self.get_gws_df(time_range=time_range)

            for index, gws in self.gws_df.iterrows():
                consortium = "other"
                gws_name = gws['VolumeName'].split('/')[-1]
                gws_vol = gws['VolumeName'].split('/')[-1]
                if re.match( 'vol\d', gws_name):
                    gws_name = gws['VolumeName'].split('/')[-2]
                for index, line in self.gws_consortium.iterrows():
                    gws_name_match = re.sub('_vol\d', '', gws_name)
                if 'wiser' in gws_name_match:
                    gws_name_match = 'wiser'
                elif 'primavera' in gws_name_match:
                    gws_name_match = 'primavera'
                if line['gws_name'] == gws_name_match:
                    consortium = line['consortium']
                vol_type = gws['VolumeType']
                gws_name_to_pass = gws['VolumeName']

                yield {
                    "_index": "mjones-test2",
                    "_source": {
                        "@timestamp": d,
                        "prometheus": {
                            "metrics": {'storage_gws_provision': float(self.gws_df[self.gws_df['VolumeName'] == gws_name_to_pass]['VolumeCapacityGB'].values[0])
                                        },
                            "labels": {
                                "gws_vol": gws_vol, 
                                "gws_name": gws_name_match, 
                                "consortium": consortium, 
                                "volume_type": vol_type,
                                "metric_name": 'storage_gws_provision',
                                "instance": "localhost:8091",
                                "job": "prometheus"
                            }
                        },
                        "event": {
                            "duration": 5425153946,
                            "dataset": "prometheus.collector",
                            "module": "prometheus"
                        },
                        "metricset": {
                            "period": 1200000,
                            "name": "collector"
                        },
                        "service": {
                            "address": "localhost:8091",
                            "type": "prometheus"
                        },
                        "ecs": {
                            "version": "1.4.0"
                        },
                        "host": {
                            "hostname": "metrics1.jasmin.ac.uk",
                            "architecture": "x86_64",
                            "name": "metrics1.jasmin.ac.uk",
                            "os": {
                                "platform": "centos",
                                "version": "7 (Core)",
                                "family": "redhat",
                                "name": "CentOS Linux",
                                "kernel": "3.10.0-1062.18.1.el7.x86_64",
                                "codename": "Core"
                            },
                            "containerized": False
                        },
                        "agent": {
                            "ephemeral_id": "6570074e-f1f3-4fa3-aae4-e57ff12c71e6",
                            "hostname": "metrics1.jasmin.ac.uk",
                            "id": "515c3f57-1204-4a41-b84e-43c08b206a74",
                            "version": "7.6.2",
                            "type": "metricbeat"
                        }
                    }
                }
              

    