import pandas as pd
import django
from django.core import serializers
import os
from jasmin_services.models import Grant, Service, Role, Category
from jasmin_auth.models import JASMINUser, Institution
import datetime, time

class UsersMetrics:
    def __init__(self):
        
        
        self.today = datetime.datetime.now()

        # generate pandas frame for all services and pks
        self.services = self.get_services()
        self.roles = self.get_roles()
        self.categories = self.get_categories()
        self.update_time = time.time()

    def check_last_frame_gen(self):
        # the generation of the pandas frames is in the init, so we need to check that these have been updated recently
        # enough
        now = time.time()
        if now-self.update_time > 7200:
            # generate pandas frame for all services and pks
            self.services = self.get_services()
            self.roles = self.get_roles()
            self.categories = self.get_categories()
            # update the update time
            self.update_time = time.time()

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
        self.check_last_frame_gen()
        return list(self.services[self.services['category'] == self.categories['group_workspaces']]['name'])

    def get_list_vms_project(self):
        self.check_last_frame_gen()
        return list(self.services[self.services['category'] == self.categories['project_vms']]['name'])

    def get_list_tenancies(self):
        self.check_last_frame_gen()
        return list(self.services[self.services['category'] == self.categories['cloud_tenancies']]['name'])

    def get_role_pk(self, service, category, role):
        self.check_last_frame_gen()
        # get the service primary key
        service_pk = int(self.services[self.services['name'] == service][self.services['category'] == category]['pk'])
        # get the role primary key
        role_pk = self.roles[self.roles['service'] == service_pk][self.roles['name'] == role]['pk']

        return int(role_pk)

    def get_active_today(self, service, category, role):
        self.check_last_frame_gen()
        count = Grant.objects.order_by() \
            .filter(role=self.get_role_pk(service, category, role)) \
            .filter(revoked=False, expires__gte=self.today,
                    granted_at__lt=self.today) \
            .distinct('user').count()
        return count

    def get_users_jasmin_login_active_today(self):
        self.check_last_frame_gen()
        return self.get_active_today('jasmin-login', self.categories['login_services'], 'USER')

    def get_users_jasmin_login_new_quarter(self):
        self.check_last_frame_gen()

        count = Grant.objects.order_by()\
                     .filter(role = self.get_role_pk('jasmin-login', self.categories['login_services'], 'USER'))\
                     .filter(granted_at__gte = self.today - datetime.timedelta(days=90),
                             granted_at__lte = self.today)\
                             .distinct('user').count()
        return count
    
    def get_users_jasmin_login_new_week(self):
        self.check_last_frame_gen()

        count = Grant.objects.order_by()\
                     .filter(role = self.get_role_pk('jasmin-login', self.categories['login_services'], 'USER'))\
                     .filter(granted_at__gte = self.today - datetime.timedelta(days=7),
                             granted_at__lte = self.today)\
                             .distinct('user').count()
        return count

    def get_new_today(self, service_name, category, type):
        self.check_last_frame_gen()
        count = Grant.objects.order_by() \
            .filter(role=self.get_role_pk(service_name, category, type)) \
            .filter(granted_at__gte=self.today,
                    granted_at__lte=self.today) \
            .distinct('user').count()
        return count

    def get_users_jasmin_login_new_today(self):
        self.check_last_frame_gen()
        return self.get_new_today('jasmin-login', self.categories['login_services'], 'USER')


    def get_users_gws_active_today(self, gws_name):
        self.check_last_frame_gen()
        return self.get_active_today(gws_name, self.categories['group_workspaces'], 'USER')

    def get_users_cloud_active_today(self, tenancy):
        self.check_last_frame_gen()
        return self.get_active_today(tenancy, self.categories['cloud_tenancies'], 'USER')

    def get_users_vm_active_today(self, vm):
        self.check_last_frame_gen()
        return self.get_active_today(vm, self.categories['project_vms'], 'USER')

    def get_users_storage_total_unique_today(self):
        self.check_last_frame_gen()
        # list of users
        users = []
        for gws in self.get_list_gws():
            grants = Grant.objects.order_by() \
                .filter(role=self.get_role_pk(gws, self.categories['group_workspaces'], 'USER')) \
                .filter(revoked=False, expires__gte=self.today,
                        granted_at__lt=self.today) \
                .distinct('user')
            for u in grants:
                users.append(u.user.username)
        # set removes duplicates
        users_set = set(users)

        return len(users_set)

    def get_list_institution(self):
        return Institution.objects.all()

    def get_list_countries(self):
        inst_list = self.get_list_institution()
        countries = []
        for i in inst_list:
            countries.append(i.get_country_display())

        countries.sort()
        countries_set = set(countries)

        return countries_set

    def get_list_disciplines(self):
        user_list = JASMINUser.objects.all()
        disc = []
        for i in user_list:
            disc.append(i.discipline)

        disc_set = set(disc)

        return disc_set

    def get_users_jasmin(self):
        return JASMINUser.objects.count()

    def get_users_jasmin_institution(self, institution):
        # count the number of users with that institution
        count = JASMINUser.objects.filter(institution=institution.pk).count()

        return count

    def get_users_jasmin_discipline(self, discipline):
        # count = 0
        # for u in JASMINUser.objects.all():
        #     if u.discipline == discipline:
        #         count += 1
        count = JASMINUser.objects.filter(discipline=discipline).count()
        return count

    def get_users_jasmin_country(self, country):
        # list the relavent institutions
        inst_rel = []
        inst_all = self.get_list_institution()
        for i in inst_all:
            if i.get_country_display() == country:
                inst_rel.append(i)
        inst_rel = set(inst_rel)

        users = 0
        for i in inst_rel:
            users += JASMINUser.objects.filter(institution=i.pk).count()

        return users
