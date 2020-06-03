import pandas as pd
import django
from django.core import serializers
import os
from jasmin_services.models import Grant, Service, Role
from jasmin_auth.models import JASMINUser
import datetime

class UsersMetrics:
    def __init__(self):
        
        
        self.today = datetime.datetime.now()

        # generate pandas frame for all services and pks

        self.services = self.get_services()
        #self.grants = self.get_grants()
        self.roles = self.get_roles()

    def get_services(self):
        # generate serialised object to iterat through
        data = serializers.serialize("python", Service.objects.all())
        pks = []
        names = []
        category = []
        for s in data:
            pks.append(s['pk'])
            names.append(s['fields']['name'])
            category.append(s['fields']['category'])

        df = pd.DataFrame()
        df['pk'] = pks
        df['name'] = names
        df['category'] = category

        return df

    def get_roles(self):
        # generate serialised object to iterat through
        data = serializers.serialize("python", Role.objects.all())
        pks = []
        names = []
        service = []
        for s in data:
            pks.append(s['pk'])
            names.append(s['fields']['name'])
            service.append(s['fields']['service'])
 
        df = pd.DataFrame()
        df['pk'] = pks
        df['name'] = names
        df['service'] = service

        return df

    def get_role_pk(self, service, role):
        # get the service primary key
        service_pk = self.services[self.services['name']==service]['pk'][0]
        # get the role primary key
        role_pk = self.roles[self.roles['service']==service_pk][self.roles['name']==role]['pk'][0]

        return int(role_pk)


    def get_users_jasmin_login_active_today(self):
        count = Grant.objects.order_by()\
                    .filter(role = self.get_role_pk('jasmin-login','USER'))\
                    .filter(revoked = False, expires__gte = self.today, 
                            granted_at__lt = self.today)\
                           .distinct('user').count()
        return count

    def get_users_jasmin_login_new_quarter(self):

        count = Grant.objects.order_by()\
                     .filter(role = self.get_role_pk('jasmin-login', 'USER'))\
                     .filter(granted_at__gte = self.today - datetime.timedelta(days=90),
                             granted_at__lte = self.today)\
                             .distinct('user').count()
        return count
    
    def get_users_jasmin_login_new_week(self):

        count = Grant.objects.order_by()\
                     .filter(role = self.get_role_pk('jasmin-login', 'USER'))\
                     .filter(granted_at__gte = self.today - datetime.timedelta(days=7),
                             granted_at__lte = self.today)\
                             .distinct('user').count()
        return count


    def get_users_jasmin_login_new_today(self):

        count = Grant.objects.order_by()\
                     .filter(role = self.get_role_pk('jasmin-login', 'USER'))\
                     .filter(granted_at__gte = self.today,
                             granted_at__lte = self.today)\
                             .distinct('user').count()
        return count
        
        
    
    def get_users_jasmin(self):
        return JASMINUser.objects.count()

