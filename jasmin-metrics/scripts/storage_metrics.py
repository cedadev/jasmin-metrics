import scripts.utils as ut

class storage_metrics:

    def __init__(self):
        self.client = ut.get_influxdb_client()

    def get_scd_last_elt(self):
        """ Get the last storage from 'EquallogicTotal' measurements.
        """

        last_res = self.client.query('select * from EquallogicTotals where time > now() - 1d')
        raw_data = last_res.raw['series']

        data = {}
        for d in raw_data[0]['values']:
            data['{}-{}'.format(d[-1], d[1])] = {
                'time': d[0],
                'rawcap': d[2],
                'rawuse': d[4],
            }
        return data


    def get_scd_last_st(self):
        """ Get the details of the last volume total usage from SCD's InfluxDB. From 'StorageTotals' measurements.
        """

        # last_res = client.query('select * from StorageTotals group by * order by desc limit 1')
        last_res = self.client.query('select * from StorageTotals WHERE time > now() - 1d')
        # print (last_res)# raw passes back a dict object
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


    def get_storage_summary(self):
        # returns the total PFS available
        # Scaled by 1.3 to account for RAID6+

        data = self.get_scd_last_st()

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
        total['tot-use'] = total['PFS-use'] + total['QB-use'] #+ total['EL-use'] + get_openstack_vms_storage_used()

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
