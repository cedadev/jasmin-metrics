from scripts.xdmod import XdMOD
from scripts.utils import *
import datetime
from pprint import pprint
import pandas as pd
from influxdb import DataFrameClient

class BackfillLotus(object):
    def __init__(self):
        self.xdmod = XdMOD()
        self.client =  DataFrameClient('influxdb1.jasmin.ac.uk', '8086',  database='test')
    def today(self):
        return datetime.datetime.now().strftime('%Y-%m-%d')

    def get_metric(self, metric, start, end):
        param = self.xdmod.get_tsparam(metric, start_date=start, end_date=end)
        return param

    def get_daily(self, metric, start, end):
        # Gather the daily metric data for the period, which need to be done a month at a time
        curr_start = datetime.datetime.strptime(start,'%Y-%m-%d')
        curr_end = curr_start+datetime.timedelta(days=29)

        strf_end = curr_end.strftime('%Y-%m-%d')
        # get first dataframe
        data = [] 

        # get data for each month and add to data frame
        while curr_end <= datetime.datetime.strptime(end,'%Y-%m-%d'):
            strf_start = curr_start.strftime('%Y-%m-%d')
            strf_end = curr_end.strftime('%Y-%m-%d')
            curr_df = self.get_metric(metric, strf_start, strf_end)
            curr_data = curr_df[metric]

            data.extend(curr_data)
            
            curr_start = curr_end+datetime.timedelta(days=1)
            curr_end = curr_start+datetime.timedelta(days=30)

        strf_start = curr_start.strftime('%Y-%m-%d')
        curr_df = self.get_metric(metric, strf_start, end)
        curr_data = curr_df[metric]
        data.extend(curr_data)

        return data
     
    def write_df_influxdb_daily(self,xdmod_metric,influx_metric,start,end):
        data = self.get_daily(xdmod_metric, start, end)
        df = pd.DataFrame(data=data,
                      index=pd.date_range(start=start,
                                          end=end, freq='d'), columns=['gauge'])
        self.client.write_points(df, influx_metric, protocol='line')

    def write_df_influxdb_monthly(self,xdmod_metric,influx_metric,start,end):
        data = self.get_daily(xdmod_metric, start, end)
        df = pd.DataFrame(data=data,
                      index=pd.date_range(start=start,
                                          end=end, freq='d'), columns=['gauge'])
        df_m = (df.groupby(pd.PeriodIndex(df.index, freq='m')).sum())
        self.client.write_points(df_m, influx_metric, protocol='line')

class BackfillStorage(object):
    def __init__(self):
        self.write_client =  DataFrameClient('influxdb1.jasmin.ac.uk', '8086',  database='test')
        self.scale_factor = 10 ** 3
        self.client = get_influxdb_client()

    def get_data(self, start, end):
        query = "select * from StorageTotals where time >=  '{}T00:00:00Z' and time <= '{}T00:00:00Z' ".format(start, end)
        data = self.client.query(query)

        return data.raw

    def get_df_data(self, start, end):
        data = self.get_data(start, end)
        pf_index = []
        qb_index = []
        pf_com = []
        pf_cap = []
        qb_com = []
        qb_cap = []
        
        for vals in data['series'][0]['values']:
            if 'PAN' in vals[-1]:
                pf_index.append(datetime.datetime.strptime(vals[0],'%Y-%m-%dT%H:%M:%SZ'))
                pf_cap.append(vals[5] / 1.3 / self.scale_factor)
                pf_com.append(vals[6] / 1.3 / self.scale_factor)

            elif 'QB' in vals[-1]:
                qb_index.append(datetime.datetime.strptime(vals[0],'%Y-%m-%dT%H:%M:%SZ'))
                # worked out from mon dashboard
                qb_cap.append(vals[5] / self.scale_factor)
                qb_com.append(vals[7] / self.scale_factor)

        
        df_pan = pd.DataFrame(data=pf_cap,index=pf_index,columns=['cap'])
        df_pan['com'] = pf_com
        df_pan = df_pan.groupby(df_pan.index).sum()

        df_qb = pd.DataFrame(data=qb_cap, index=qb_index, columns=['cap'])
        df_qb['com'] = qb_com
        df_qb = df_qb.groupby(df_qb.index).sum()

        return df_pan, df_qb

    def write_indv_totals_influx(self,start,end):
        df_pan, df_qb = self.get_df_data(start, end)
        df_pan= df_pan.groupby(df_pan.index.week).tail(1)
        df_qb = df_qb.groupby(df_qb.index.week).tail(1)
        
        df_pan_cap = pd.DataFrame(index=df_pan.index)
        df_pan_cap['gauge'] =df_pan['cap'] 
        df_pan_com = pd.DataFrame(index=df_pan.index)
        df_pan_com['gauge'] = df_pan['com']

        self.write_client.write_points(df_pan_cap, 'storage_pfs_total', protocol='line')
        self.write_client.write_points(df_pan_com, 'storage_pfs_used', protocol='line')

        df_qb_cap = pd.DataFrame(index=df_qb.index)
        df_qb_cap['gauge'] = df_qb['cap']
        df_qb_com = pd.DataFrame(index=df_qb.index)
        df_qb_com['gauge'] = df_qb['com']

        self.write_client.write_points(df_qb_cap, 'storage_sof_total', protocol='line')
        self.write_client.write_points(df_qb_com, 'storage_sof_used', protocol='line')

    def write_totals_influx(self,start,end):
        df_pan, df_qb = self.get_df_data(start, end)


        df_pan = df_pan.groupby([df_pan.index.year, df_pan.index.month]).tail(1)
        df_qb = df_qb.groupby([df_qb.index.year, df_qb.index.month]).tail(1)
        df_tot = pd.concat([df_pan,df_qb])

        df_tot = df_tot.groupby(df_tot.index.month).sum()
        df_tot.index = pd.date_range(start=start,
                                          end=end, freq='M')
        df_tot_cap = pd.DataFrame(index=df_tot.index)
        df_tot_cap['gauge'] = df_tot['cap']
        df_tot_com = pd.DataFrame(index=df_tot.index)
        df_tot_com['gauge'] = df_tot['com']

        self.write_client.write_points(df_tot_cap, 'storage_total', protocol='line')
        self.write_client.write_points(df_tot_com, 'storage_com', protocol='line')

def backfill_storage():
    bf = BackfillStorage()

    bf.write_indv_totals_influx('2019-01-01','2019-12-31')
    bf.write_totals_influx('2019-01-01','2019-12-31')

def backfill_lotus():
    bf = BackfillLotus()
    bf.write_df_influxdb_daily('job_count', 'lotus_job_count_finished_day','2020-01-01', '2020-02-06')
    bf.write_df_influxdb_monthly('job_count', 'lotus_job_count_finished_month','2017-01-01', '2020-01-31') 

if __name__ == "__main__":
   #backfill_storage()
