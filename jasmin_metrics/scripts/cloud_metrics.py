from .utils import *
import numpy as np
import pandas as pd

class CloudMetrics:

    def __init__(self):

        self.client = get_influxdb_client()
        self.ten_df = self.get_ten_df()

    def get_ten_df(self):
        last_res = self.client.query('select * from Openstack where time > now() - 1h') #seems to be a report every hour ish
        if len(last_res) == 0:
            last_res = self.client.query('select * from Openstack where time > now() - 2h')
        if len(last_res) == 0:
            last_res = self.client.query('select * from Openstack where time > now() - 3h')


        raw = last_res.raw['series'][0]
        cols = raw['columns']
        data = raw['values']
        df = pd.DataFrame(data, columns=cols)

        return df
    

    def get_cloud_count(self, suffix):
        # count the _U in the tenancy names
        count = 0
        for i,n in self.ten_df.iterrows():
            if n['Project_Name'].endswith(suffix):
                count += 1
        return count

    def get_cloud_managed_count(self):

        return self.get_cloud_count('-M')

    def get_cloud_external_count(self):

        return self.get_cloud_count('-U')

    def get_cloud_special_count(self):

        return self.get_cloud_count('-S')

    def get_cloud_tenancy_vcpus_quota(self, ten):
        return float(self.ten_df[self.ten_df['Project_Name'] == ten]['VCPUs_Quota'].values[0])

    def get_cloud_tenancy_vcpus_used(self, ten):
        return float(self.ten_df[self.ten_df['Project_Name'] == ten]['VCPUs_Used'].values[0])

    def get_cloud_tenancy_ram_quota(self, ten):
        return float(self.ten_df[self.ten_df['Project_Name'] == ten]['RAM_Quota_MiB'].values[0])

    def get_cloud_tenancy_ram_used(self, ten):
        return float(self.ten_df[self.ten_df['Project_Name'] == ten]['RAM_Used_MiB'].values[0])

    def get_cloud_tenancy_storage_quota(self, ten):
        return float(self.ten_df[self.ten_df['Project_Name'] == ten]['Storage_Quota_GiB'].values[0])

    def get_cloud_tenancy_instance_storage_used(self, ten):
        return float(self.ten_df[self.ten_df['Project_Name'] == ten]['Instance_Storage_Used_GiB'].values[0])

    def get_cloud_tenancy_volume_storage_used(self, ten):
        return float(self.ten_df[self.ten_df['Project_Name'] == ten]['Volume_Storage_Used_GiB'].values[0])

    def get_cloud_tenancy_instance_count(self, ten):
        return float(self.ten_df[self.ten_df['Project_Name'] == ten]['Instance_Count'].values[0])


    
