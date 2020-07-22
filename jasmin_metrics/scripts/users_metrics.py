import pandas as pd
import django
from django.core import serializers
import os
from jasmin_services.models import Grant, Service, Role, Category
from jasmin_auth.models import JASMINUser, Institution
import datetime
import time


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
        service_pk = int(self.services[self.services['name'] ==
                                       service][self.services['category'] == category]['pk'])
        # get the role primary key
        role_pk = self.roles[self.roles['service'] ==
                             service_pk][self.roles['name'] == role]['pk']

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
                     .filter(role=self.get_role_pk('jasmin-login', self.categories['login_services'], 'USER'))\
                     .filter(granted_at__gte=self.today - datetime.timedelta(days=90),
                             granted_at__lte=self.today)\
            .distinct('user').count()
        return count

    def get_users_jasmin_login_new_week(self):
        self.check_last_frame_gen()

        count = Grant.objects.order_by()\
                     .filter(role=self.get_role_pk('jasmin-login', self.categories['login_services'], 'USER'))\
                     .filter(granted_at__gte=self.today - datetime.timedelta(days=7),
                             granted_at__lte=self.today)\
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


class UsersBackfill(UsersMetrics):
    """ This class uses the same format as the UsersMetrics class, but returns the generators required to upload history to ES
    """

    def __init__(self):
        super().__init__()

    def get_active(self, service, category, role, start, end):
        self.check_last_frame_gen()
        count = Grant.objects.order_by() \
            .filter(role=self.get_role_pk(service, category, role)) \
            .filter(revoked=False, expires__gte=start,
                    granted_at__lt=end) \
            .distinct('user').count()
        return count

    def get_jasmin_users_upto(self, date):
        # counts number ofg jasmin accounts which would have existed at the date supplied since 2001
        start_date = datetime.datetime.strptime('2001-01-01', '%Y-%m-%d')
        return JASMINUser.objects.filter(date_joined__gte=start_date, date_joined__lt=date).count()

    def get_users_jasmin(self, start, end):
        dates = self._gen_time_list(start, end)

        for d in dates:
            t_dt = datetime.datetime.strptime(d, '%Y-%m-%dT%H:%M:%SZ')
            yield {
                "_index": "mjones-test2",
                "_source": {
                    "@timestamp": d,
                    "prometheus": {
                        "metrics": {'users_jasmin': self.get_jasmin_users_upto(t_dt)
                                    },
                        "labels": {
                            "instance": "localhost:8091",
                            "job": "prometheus"
                        }
                    },
                    "event": {
                        "duration": 5425153946,
                        "dataset": "prometheus.collector",
                        "module": "prometheus"
                    },
                    "metricset": {
                        "period": 1200000,
                        "name": "collector"
                    },
                    "service": {
                        "address": "localhost:8091",
                        "type": "prometheus"
                    },
                    "ecs": {
                        "version": "1.4.0"
                    },
                    "host": {
                        "hostname": "metrics1.jasmin.ac.uk",
                        "architecture": "x86_64",
                        "name": "metrics1.jasmin.ac.uk",
                        "os": {
                            "platform": "centos",
                            "version": "7 (Core)",
                            "family": "redhat",
                            "name": "CentOS Linux",
                            "kernel": "3.10.0-1062.18.1.el7.x86_64",
                            "codename": "Core"
                        },
                        "containerized": False
                    },
                    "agent": {
                        "ephemeral_id": "6570074e-f1f3-4fa3-aae4-e57ff12c71e6",
                        "hostname": "metrics1.jasmin.ac.uk",
                        "id": "515c3f57-1204-4a41-b84e-43c08b206a74",
                        "version": "7.6.2",
                        "type": "metricbeat"
                    }
                }
            }
        return JASMINUser.objects.count()

    def _gen_time_list(self, start, end):
        datelist = pd.date_range(
            start=start+'T15:04:45.325Z', end=end+'T15:04:45.325Z')
        return datelist.strftime('%Y-%m-%dT%H:%M:%SZ').tolist()

    def get_users_jasmin_login_active_today(self, start, end):
        dates = self._gen_time_list(start, end)

        for d in dates:
            t_dt = datetime.datetime.strptime(d, '%Y-%m-%dT%H:%M:%SZ')
            yield {
                "_index": "mjones-test2",
                "_source": {
                    "@timestamp": d,
                    "prometheus": {
                        "metrics": {'users_jasmin_login_active_today': self.get_active('jasmin-login', self.categories['login_services'], 'USER', t_dt, t_dt+datetime.timedelta(days=1))
                                    },
                        "labels": {
                            "instance": "localhost:8091",
                            "job": "prometheus"
                        }
                    },
                    "event": {
                        "duration": 5425153946,
                        "dataset": "prometheus.collector",
                        "module": "prometheus"
                    },
                    "metricset": {
                        "period": 1200000,
                        "name": "collector"
                    },
                    "service": {
                        "address": "localhost:8091",
                        "type": "prometheus"
                    },
                    "ecs": {
                        "version": "1.4.0"
                    },
                    "host": {
                        "hostname": "metrics1.jasmin.ac.uk",
                        "architecture": "x86_64",
                        "name": "metrics1.jasmin.ac.uk",
                        "os": {
                            "platform": "centos",
                            "version": "7 (Core)",
                            "family": "redhat",
                            "name": "CentOS Linux",
                            "kernel": "3.10.0-1062.18.1.el7.x86_64",
                            "codename": "Core"
                        },
                        "containerized": False
                    },
                    "agent": {
                        "ephemeral_id": "6570074e-f1f3-4fa3-aae4-e57ff12c71e6",
                        "hostname": "metrics1.jasmin.ac.uk",
                        "id": "515c3f57-1204-4a41-b84e-43c08b206a74",
                        "version": "7.6.2",
                        "type": "metricbeat"
                    }
                }
            }
