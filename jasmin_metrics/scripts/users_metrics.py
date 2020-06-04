import pandas as pd
import django
from django.core import serializers
import os
from jasmin_services.models import Grant, Service, Role, Category
from jasmin_auth.models import JASMINUser
import datetime

class UsersMetrics:
    def __init__(self):
        
        
        self.today = datetime.datetime.now()

        # generate pandas frame for all services and pks
        self.services = self.get_services()
        #self.grants = self.get_grants()
        self.roles = self.get_roles()
        self.categories = self.get_categories()

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

    def get_categories(self):
        # generate serialised object to iterat through
        data = serializers.serialize("python", Category.objects.all())
        cats = {}
        for s in data:
            cats[s['fields']['name']] = s['pk']

        return cats

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

    def get_list_gws(self):
        return list(self.services[self.services['category'] == self.categories['group_workspaces']]['name'])

    def get_list_vms_project(self):
        return list(self.services[self.services['category'] == self.categories['project_vms']]['name'])

    def get_list_tenancies(self):
        return list(self.services[self.services['category'] == self.categories['cloud_tenancies']]['name'])

    def get_role_pk(self, service, category, role):
        # get the service primary key
        service_pk = int(self.services[self.services['name'] == service][self.services['category'] == category]['pk'])
        # get the role primary key
        role_pk = self.roles[self.roles['service'] == service_pk][self.roles['name'] == role]['pk']

        return int(role_pk)

    def get_active_today(self, service, category, role):
        count = Grant.objects.order_by() \
            .filter(role=self.get_role_pk(service, category, role)) \
            .filter(revoked=False, expires__gte=self.today,
                    granted_at__lt=self.today) \
            .distinct('user').count()
        return count

    def get_users_jasmin_login_active_today(self):
        return self.get_active_today('jasmin-login', self.categories['login_services'], 'USER')

    def get_users_jasmin_login_new_quarter(self):

        count = Grant.objects.order_by()\
                     .filter(role = self.get_role_pk('jasmin-login', self.categories['login_services'], 'USER'))\
                     .filter(granted_at__gte = self.today - datetime.timedelta(days=90),
                             granted_at__lte = self.today)\
                             .distinct('user').count()
        return count
    
    def get_users_jasmin_login_new_week(self):

        count = Grant.objects.order_by()\
                     .filter(role = self.get_role_pk('jasmin-login', self.categories['login_services'], 'USER'))\
                     .filter(granted_at__gte = self.today - datetime.timedelta(days=7),
                             granted_at__lte = self.today)\
                             .distinct('user').count()
        return count

    def get_new_today(self, service_name, category, type):
        count = Grant.objects.order_by() \
            .filter(role=self.get_role_pk(service_name, category, type)) \
            .filter(granted_at__gte=self.today,
                    granted_at__lte=self.today) \
            .distinct('user').count()
        return count

    def get_users_jasmin_login_new_today(self):
        return self.get_new_today('jasmin-login', self.categories['login_services'], 'USER')

    
    def get_users_jasmin(self):
        return JASMINUser.objects.count()

    def get_users_gws_active_today(self, gws_name):
        return self.get_active_today(gws_name, self.categories['group_workspaces'], 'USER')

    def get_users_cloud_active_today(self, tenancy):
        return self.get_active_today(tenancy, self.categories['cloud_tenancies'], 'USER')

    def get_users_vm_active_today(self, vm):
        return self.get_active_today(vm, self.categories['project_vms'], 'USER')


