from jasmin_metrics.scripts.storage_metrics import StorageMetrics
from jasmin_metrics.scripts.users_metrics import UsersMetrics
from jasmin_services.models import Grant
from django.http import HttpResponse
import csv
import datetime
import json
import pandas as pd


class GWSList:

    def __init__(self):
        self.df = pd.DataFrame(columns=['gws'])


    def create_view(self):
        response = HttpResponse(content_type='text/plain')

        writer = csv.writer(response)
        df = self.sm.gws_df
        for i,g in df.iterrows():
            line = [g[4]]
            writer.writerow(line)

        return response

class GWSScannerInput:

    def __init__(self):
        sm = StorageMetrics()
        self.today = datetime.datetime.today()
        # generate pandas frame for all services
        self.um = UsersMetrics()
        self.services = self.um.services
        self.roles = self.um.roles
        self.categories = self.um.categories
        self.df = pd.DataFrame()
        self.df['VolumeName'] = sm.get_gws_df(time_range='time > now() -1d')['VolumeName']
        

    
    def _get_grant_list(self, gws, role):
        grants = Grant.objects.order_by() \
            .filter(role=self.um.get_role_pk(gws, 1, role)) \
            .filter(revoked=False, expires__gte=self.today,
                    granted_at__lt=self.today) \
            .distinct('user')
        return grants


    def list_managers(self, gws):
        emails = []
        # managers
        for e in self._get_grant_list(gws, 'MANAGER'):
            emails.append(e.user.email)
        # deputies
        for e in self._get_grant_list(gws, 'DEPUTY'):
            emails.append(e.user.email)
        
        return emails
    
    def get_list_gws(self):
        return list(self.services[self.services['category'] == self.categories['group_workspaces']]['name'])


    def gen_managers_list(self):
        disp_dict = {}
        gws_list = self.get_list_gws()
        for gws in gws_list:
            managers = self.list_managers(gws)
            # try and get the path for the GWS
            gws_paths = self.df[self.df['VolumeName'].str.contains('/'+gws)]['VolumeName']
            managers_str = ','.join(managers)
            disp_dict[gws] = {}
            disp_dict[gws]['managers'] = managers_str
            volumes_str = ','.join(gws_paths.to_list())
            # exceptions which are not caught with the above
            # generally this is where the gws matches other gwss which start with its name
            if gws =='ukesm':
                volumes_str = '/group_workspaces/jasmin2/ukesm'
            elif gws == 'higem':
                volumes_str = '/group_workspaces/jasmin4/higem'
            elif gws == 'jules':
                volumes_str = '/gws/nopw/j04/jules'
            elif gws == 'swift':
                volumes_str = '/gws/nopw/j04/swift'

            disp_dict[gws]['paths'] = volumes_str

        return disp_dict

    def create_view(self):
        #response = HttpResponse(content=self.gen_managers_list(), content_type='text/plain')
        response = HttpResponse(json.dumps(self.gen_managers_list()), content_type="application/json")
        #response['Content-Disposition'] = 'attachment; filename="volume_report.csv"'
        return response


class VolumeReport:

    def __init__(self):
        self.sm = StorageMetrics()


    def create_view(self):
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="volume_report.csv"'

        writer = csv.writer(response)
        writer.writerow(['Realm', 'Path', 'Used TB', 'Available TB','Size TB'])
        for df in [self.sm.gws_df, self.sm.get_archive_df(time_range="time > now() - 1d")]:
            for i,g in df.iterrows():
                line = [g[1],
                        g[4],
                        g[5]/10**3,
                        (g[2]-g[5])/10**3,
                        g[2]/10**3]
                writer.writerow(line)

        return response

class GWSUsersReport:
    def __init__(self):
        self.users = UsersMetrics()

    def create_view(self):
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="gws_users_report.csv"'

        writer = csv.writer(response)
        writer.writerow(['GWS Name', 'Users'])
        for name in self.users.get_list_gws():
            writer.writerow([name, self.users.get_users_gws_active_today(name)])

        return response