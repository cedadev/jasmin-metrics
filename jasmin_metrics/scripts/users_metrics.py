

import django
import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "jasmin_account_site.settings")
django.setup()
from jasmin_services.models import Grant
from jasmin_auth.models import JASMINUser
import datetime

class UsersMetrics:
    def __init__(self):
        
        
        self.today = datetime.datetime.now()
        self.grant_dict = {'jasmin_login': 1,
                          }

    def get_users_jasmin_login_active_today(self):
        count = Grant.objects.order_by()\
                    .filter(role = self.grant_dict['jasmin_login'])\
                    .filter(revoked = False, expires__gte = self.today, 
                            granted_at__lt = self.today)\
                           .distinct('user').count()
        return count

    def get_users_jasmin_login_new_quarter(self):

        count = Grant.objects.order_by()\
                     .filter(role = self.grant_dict['jasmin_login'])\
                     .filter(granted_at__gte = self.today - datetime.timedelta(days=90),
                             granted_at__lte = self.today)\
                             .distinct('user').count()
        return count
    
    def get_users_jasmin_login_new_week(self):

        count = Grant.objects.order_by()\
                     .filter(role = self.grant_dict['jasmin_login'])\
                     .filter(granted_at__gte = self.today - datetime.timedelta(days=7),
                             granted_at__lte = self.today)\
                             .distinct('user').count()
        return count


    def get_users_jasmin_login_new_today(self):

        count = Grant.objects.order_by()\
                     .filter(role = self.grant_dict['jasmin_login'])\
                     .filter(granted_at__gte = self.today,
                             granted_at__lte = self.today)\
                             .distinct('user').count()
        return count
        
        
    
    def get_users_jasmin(self):
        return JASMINUser.objects.count()

if __name__ == "__main__":
    um = UsersMetrics()
    print(um.get_users_jasmin_login_new_quarter())
    print(um.get_users_jasmin_login_new_week())
    print(um.get_users_jasmin_login_new_today())
