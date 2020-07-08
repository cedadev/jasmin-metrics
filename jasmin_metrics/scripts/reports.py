from jasmin_metrics.scripts.storage_metrics import StorageMetrics
from jasmin_metrics.scripts.users_metrics import UsersMetrics
from django.http import HttpResponse
import csv

class VolumeReport:

    def __init__(self):
        self.sm = StorageMetrics()


    def create_view(self):
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = 'attachment; filename="volume_report.csv"'

        writer = csv.writer(response)
        writer.writerow(['Realm', 'Path', 'Used TB', 'Available TB','Size TB'])
        for df in [self.sm.gws_df, self.sm.get_archive_df()]:
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