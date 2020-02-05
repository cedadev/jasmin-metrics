import utils as ut

class mc_metrics:

    def __init__(self):

        self.client = ut.get_influxdb_client()

    def get_openstack_vms_count(self):

        q = self.client.query('select * from Openstack where time > now() - 1h') #seems to be a report every hour ish
        if len(q) == 0:
            q = self.client.query('select * from Openstack where time > now() - 2h')
        count = len([x[2] for x in q.raw['series'][0]['values']])
        return count

    def get_openstack_vms_cpus_quota(self):
        q = self.client.query('select VCPUs_Quota from Openstack where time > now() - 1h')
        if len(q) == 0:
            q = self.client.query('select VCPUs_Quota from Openstack where time > now() - 2h')

        tot = np.sum([x[1] for x in q.raw['series'][0]['values']])

        return tot

    def get_openstack_vms_ram_quota(self):
        q = self.client.query('select RAM_Quota_MiB from Openstack where time > now() - 1h')
        if len(q) == 0:
            q = self.client.query('select RAM_Quota_MiB from Openstack where time > now() - 2h')
        tot = np.sum([x[1] for x in q.raw['series'][0]['values']])
        return tot/10**3

    def get_openstack_vms_storage_quota(self):
        q = self.client.query('select Storage_Quota_GiB from Openstack where time > now() - 1h')
        if len(q) == 0:
            q = self.client.query('select Storage_Quota_GiB from Openstack where time > now() - 2h')
        tot = np.sum([x[1] for x in q.raw['series'][0]['values']])
        return tot/10**3

    def get_openstack_vms_cpus_used(self):
        client = get_influxdb_client()
        q = self.client.query('select VCPUs_Used from Openstack where time > now() - 1h')
        if len(q) == 0:
            q = self.client.query('select VCPUs_Used from Openstack where time > now() - 2h')
        tot = np.sum([x[1] for x in q.raw['series'][0]['values']])
        return tot

    def get_openstack_vms_ram_used(self):
        q = self.client.query('select RAM_Used_MiB from Openstack where time > now() - 1h')

        if len(q) == 0:
            q = self.client.query('select RAM_Used_MiB from Openstack where time > now() - 2h')
        tot = np.sum([x[1] for x in q.raw['series'][0]['values']])
        return tot/10**3

    def get_openstack_vms_storage_used(self):

        q = self.client.query('select Volume_Storage_Used_GiB from Openstack where time > now() - 1h')
        if len(q) == 0:
            q = self.client.query('select Volume_Storage_Used_GiB from Openstack where time > now() - 2h')
        tot = np.sum([x[1] for x in q.raw['series'][0]['values']])
        return tot/10**3