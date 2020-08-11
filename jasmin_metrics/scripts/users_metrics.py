import pandas as pd
import django
from django.core import serializers
import os
from jasmin_services.models import Grant, Service, Role, Category
from jasmin_auth.models import JASMINUser, Institution
import datetime
import time
from .utils import *

class UsersGather:
    """ Class to contain all the gathering methods for the metrics. Should be time period indfependant
    """

    def __init__(self):

        # generate pandas frame for all services and pks
        self.services = self.get_services()
        self.roles = self.get_roles()
        self.categories = self.get_categories()
        self.update_time = time.time()

        self.today = datetime.datetime.now()
        self.start_dt = datetime.datetime.strptime('2000-01-01', '%Y-%m-%d')

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

    def gather_active(self, service, category, role, start, end):
        self.check_last_frame_gen()
        count = Grant.objects.order_by() \
            .filter(role=self.get_role_pk(service, category, role)) \
            .filter(revoked=False, expires__gte=start,
                    granted_at__lt=end) \
            .distinct('user').count()
        return count

    def gather_new(self, service_name, category, type, start, end):
        self.check_last_frame_gen()
        count = Grant.objects.order_by() \
            .filter(role=self.get_role_pk(service_name, category, type)) \
            .filter(granted_at__gte=start,
                    granted_at__lte=end) \
            .distinct('user').count()
        return count

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

    def gather_users_storage_total_unique(self, start, end):
        self.check_last_frame_gen()
        # list of users
        users = []
        for gws in self.get_list_gws():
            grants = Grant.objects.order_by() \
                .filter(role=self.get_role_pk(gws, self.categories['group_workspaces'], 'USER')) \
                .filter(revoked=False, expires__gte=start,
                        granted_at__lt=end) \
                .distinct('user')
            for u in grants:
                users.append(u.user.username)
        # set removes duplicates
        users_set = set(users)

        return len(users_set)

    def gather_users_jasmin_country(self, country, start, end):
        # list the relavent institutions
        inst_rel = []
        inst_all = self.get_list_institution()
        for i in inst_all:
            if i.get_country_display() == country:
                inst_rel.append(i)
        inst_rel = set(inst_rel)

        users = 0
        for i in inst_rel:
            users += JASMINUser.objects.filter(institution=i.pk,
                                               date_joined__gte=start, date_joined__lt=end).count()

        return users

    def gather_users_jasmin_discipline(self, discipline, start, end):
        count = JASMINUser.objects.filter(
            discipline=discipline, date_joined__gte=start, date_joined__lt=end).count()
        return count

    def gather_users_jasmin_institution(self, institution, start, end):
        # count the number of users with that institution
        count = JASMINUser.objects.filter(
            institution=institution.pk, date_joined__gte=start, date_joined__lt=end).count()

        return count
    
    def gather_users_jasmin(self, start, end):
        # count the number of users with that institution
        count = JASMINUser.objects.filter(date_joined__gte=start, date_joined__lt=end).count()

        return count


class UsersMetrics(UsersGather):
    """ Class which provides the metrics for injestion with prometheus.
    """

    def __init__(self):
        super().__init__()

    def get_users_jasmin(self):
        return self.gather_users_jasmin(self.start_dt, self.today)

    def get_users_jasmin_institution(self, institution):
        # get the total number of users for specific institution (2000 start date captures total count)
        return self.gather_users_jasmin_institution(institution, self.start_dt, self.today)

    def get_users_jasmin_discipline(self, discipline):
        # get the total number of users for specific discipline (2000 start date captures total count)
        return self.gather_users_jasmin_discipline(discipline, self.start_dt, self.today)

    def get_users_jasmin_country(self, country):
        # get the total number of users for specific country (2000 start date captures total count)
        return self.gather_users_jasmin_country(country, self.start_dt, self.today)

    def get_users_jasmin_login_new_today(self):
        return self.gather_new('jasmin-login', self.categories['login_services'], 'USER', self.today, self.today)

    def get_users_gws_active_today(self, gws_name):
        return self.gather_active(gws_name, self.categories['group_workspaces'], 'USER', self.today, self.today)

    def get_users_cloud_active_today(self, tenancy):
        return self.gather_active(tenancy, self.categories['cloud_tenancies'], 'USER', self.today, self.today)

    def get_users_vm_active_today(self, vm):
        return self.gather_active(vm, self.categories['project_vms'], 'USER', self.today, self.today)

    def get_users_storage_total_unique_today(self):
        return self.gather_users_storage_total_unique(self.today, self.today)

    def get_users_jasmin_login_active_today(self):
        return self.gather_active('jasmin-login', self.categories['login_services'], 'USER', self.today, self.today)

    def get_users_jasmin_login_new_quarter(self):
        return self.gather_new('jasmin-login', self.categories['login_services'], 'USER', self.today - datetime.timedelta(days=90), self.today)

    def get_users_jasmin_login_new_week(self):
        return self.gather_new('jasmin-login', self.categories['login_services'], 'USER', self.today - datetime.timedelta(days=7), self.today)
        return count

class UsersBackfill(UsersGather):
    """ This class uses the same format as the UsersMetrics class, but returns the generators required to upload history to ES
    """

    def __init__(self):
        super().__init__()


    def get_users_gws_active_today(self, start, end):
        
        dates = gen_time_list(start, end)
        for d in dates:
            t_dt = datetime.datetime.strptime(d, '%Y-%m-%dT%H:%M:%SZ')
            for name in self.get_list_gws():
            
                yield {
                    "_index": "mjones-test4",
                    "_source": {
                        "@timestamp": d,
                        "prometheus": {
                            "metrics": {'users_gws_active_today': self.gather_active(name, self.categories['group_workspaces'], 'USER', t_dt, t_dt)
                                        },
                            "labels": {
                                "gws_name": name,
                                "metric_name": 'users_gws_active_today',
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

    def get_users_jasmin(self, start, end):
        dates = gen_time_list(start, end)

        for d in dates:
            t_dt = datetime.datetime.strptime(d, '%Y-%m-%dT%H:%M:%SZ')
            yield {
                "_index": "mjones-test4",
                "_source": {
                    "@timestamp": d,
                    "prometheus": {
                        "metrics": {'users_jasmin': self.gather_users_jasmin(self.start_dt, t_dt)
                                    },
                        "labels": {
                            "metric_name": 'users_jasmin',
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

    

    def get_users_jasmin_login_active_today(self, start, end):
        dates = gen_time_list(start, end)

        for d in dates:
            t_dt = datetime.datetime.strptime(d, '%Y-%m-%dT%H:%M:%SZ')
            yield {
                "_index": "mjones-test4",
                "_source": {
                    "@timestamp": d,
                    "prometheus": {
                        "metrics": {'users_jasmin_login_active_today': self.gather_active('jasmin-login', self.categories['login_services'], 'USER', t_dt, t_dt)
                                    },
                        "labels": {
                            "metric_name": 'users_jasmin_login_active_today',
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
