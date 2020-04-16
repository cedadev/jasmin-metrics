import prometheus_client as pc
import influxdb
import configparser
from .scripts.archive_metrics import ArchiveMetrics
from .scripts.lotus_metrics import LotusMetrics
from .scripts.managed_cloud_metrics import MCMetrics
from .scripts.unmanaged_cloud_metrics import UMCMetrics
from .scripts.network_metrics import NetworkMetrics
from .scripts.storage_metrics import StorageMetrics
from .scripts.tape_metrics import TapeMetrics
#from scripts.users_metrics import UsersMetrics
from .scripts.power_metrics import PowerMetrics
import time
import os


class MetricsView(object):
    def __init__(self, metrics_group, req_metrics_file=os.environ['JASMIN_METRICS_CONFIG']):
        self.collector = pc.CollectorRegistry()
        self.metrics_group = metrics_group
        lotus = LotusMetrics()
        storage = StorageMetrics()
        mc = MCMetrics()
        arch = ArchiveMetrics()
        #users = UsersMetrics()

        self.req_metrics = self.parse_metrics_config(req_metrics_file)

        ## define dictionary of calculation functions
        #self.met_funcs = {
            #'storage_total': storage.get_storage_total,
            #'storage_used': storage.get_storage_used,
            #'storage_com': storage.get_storage_com,
            #'storage_pfs_total': storage.get_storage_pfs_total,
            #'storage_pfs_used': storage.get_storage_pfs_used,
            #'storage_sof_total': storage.get_storage_sof_total,
            #'storage_sof_used': storage.get_storage_sof_used,
            #'storage_el_total': storage.get_storage_el_total,
            #'storage_el_used': storage.get_storage_el_used,
            #'lotus_cores_count': lotus.get_lotus_cores_count,
            #'lotus_hosts_count': lotus.get_lotus_hosts_count,
            #'lotus_mem_total': lotus.get_lotus_mem_total,
            #'lotus_tbmonth_in': lotus.get_lotus_tbmonth_in,
            #'lotus_tbmonth_out': lotus.get_lotus_tbmonth_out,
            #'lotus_network_in': lotus.get_lotus_network_in,
            #'lotus_network_out': lotus.get_lotus_network_out,
            #'lotus_core_hours_day': lotus.get_lotus_core_hours_day,
            #'lotus_core_hours_3day': lotus.get_lotus_core_hours_3day,
            #'lotus_core_hours_week': lotus.get_lotus_core_hours_week,
            #'lotus_core_hours_month': lotus.get_lotus_core_hours_month,
            #'lotus_core_hours_avg_day': lotus.get_lotus_core_hours_avg_day,
            #'lotus_core_hours_avg_3day': lotus.get_lotus_core_hours_avg_3day,
            #'lotus_core_hours_avg_week': lotus.get_lotus_core_hours_avg_week,
            #'lotus_core_hours_avg_month': lotus.get_lotus_core_hours_avg_month,
            #'lotus_util_day': lotus.get_lotus_util_day,
            #'lotus_util_3day': lotus.get_lotus_util_3day,
            #'lotus_util_week': lotus.get_lotus_util_week,
            #'lotus_util_month': lotus.get_lotus_util_month,
            #'lotus_job_proc_min_day': lotus.get_lotus_job_proc_min_day,
            #'lotus_job_proc_min_3day': lotus.get_lotus_job_proc_min_3day,
            #'lotus_job_proc_min_week': lotus.get_lotus_job_proc_min_week,
            #'lotus_job_proc_min_month': lotus.get_lotus_job_proc_min_month,
            #'lotus_job_proc_avg_day': lotus.get_lotus_job_proc_avg_day,
            #'lotus_job_proc_avg_3day': lotus.get_lotus_job_proc_avg_3day,
            #'lotus_job_proc_avg_week': lotus.get_lotus_job_proc_avg_week,
            #'lotus_job_proc_avg_month': lotus.get_lotus_job_proc_avg_month,
            #'lotus_job_proc_max_day': lotus.get_lotus_job_proc_max_day,
            #'lotus_job_proc_max_3day': lotus.get_lotus_job_proc_max_3day,
            #'lotus_job_proc_max_week': lotus.get_lotus_job_proc_max_week,
            #'lotus_job_proc_max_month': lotus.get_lotus_job_proc_max_month,
            #'lotus_job_count_finished_day': lotus.get_lotus_job_count_finished_day,
            #'lotus_job_count_finished_3day': lotus.get_lotus_job_count_finished_3day,
            #'lotus_job_count_finished_week': lotus.get_lotus_job_count_finished_week,
            #'lotus_job_count_finished_month': lotus.get_lotus_job_count_finished_month,
            #'lotus_job_count_started_day': lotus.get_lotus_job_count_started_day,
            #'lotus_job_count_started_3day': lotus.get_lotus_job_count_started_3day,
            #'lotus_job_count_started_week': lotus.get_lotus_job_count_started_week,
            #'lotus_job_count_started_month': lotus.get_lotus_job_count_started_month,
            #'lotus_job_count_submitted_day': lotus.get_lotus_job_count_submitted_day,
            #'lotus_job_count_submitted_3day': lotus.get_lotus_job_count_submitted_3day,
            #'lotus_job_count_submitted_week': lotus.get_lotus_job_count_submitted_week,
            #'lotus_job_count_submitted_month': lotus.get_lotus_job_count_submitted_month,
            #'lotus_job_count_running_day': lotus.get_lotus_job_count_running_day,
            #'lotus_job_count_running_3day': lotus.get_lotus_job_count_running_3day,
            #'lotus_job_count_running_week': lotus.get_lotus_job_count_running_week,
            #'lotus_job_count_running_month': lotus.get_lotus_job_count_running_month,
            #'lotus_expansion_factor': lotus.get_lotus_expansion_factor,
            #'lotus_wait_dur_tot': lotus.get_lotus_wait_dur_tot,
            #'lotus_wait_dur_avg': lotus.get_lotus_wait_dur_avg,
            #'lotus_wall_dur_tot': lotus.get_lotus_wall_dur_tot,
            #'lotus_wall_dur_avg': lotus.get_lotus_wall_dur_avg,
            #'openstack_vms_count': mc.get_openstack_vms_count,
            #'openstack_vms_cpus_quota': mc.get_openstack_vms_cpus_quota,
            #'openstack_vms_cpus_used': mc.get_openstack_vms_cpus_used,
            #'openstack_vms_ram_quota': mc.get_openstack_vms_ram_quota,
            #'openstack_vms_ram_used': mc.get_openstack_vms_ram_used,
            #'openstack_vms_storage_quota': mc.get_openstack_vms_storage_quota,
            #'openstack_vms_storage_used': mc.get_openstack_vms_storage_used,
            #'archive_ingest1_load_1min': arch.get_archive_ingest1_load_1min,
            #'archive_ingest1_mem_swap': arch.get_archive_ingest1_mem_swap,
            #'archive_ingest2_load_1min': arch.get_archive_ingest2_load_1min,
            #'archive_ingest2_mem_swap': arch.get_archive_ingest2_mem_swap,
            #'archive_ingest3_load_1min': arch.get_archive_ingest3_load_1min,
            #'archive_ingest3_mem_swap': arch.get_archive_ingest3_mem_swap,
            #'archive_ingest4_load_1min': arch.get_archive_ingest4_load_1min,
            #'archive_ingest4_mem_swap': arch.get_archive_ingest4_mem_swap,
            #'archive_ingest5_load_1min': arch.get_archive_ingest5_load_1min,
            #'archive_ingest5_mem_swap': arch.get_archive_ingest5_mem_swap,
            #'archive_rbq_load_1min': arch.get_archive_rbq_load_1min,
            #'archive_rbq_mem_swap': arch.get_archive_rbq_mem_swap,
            #'archive_deposit1_load_1min': arch.get_archive_deposit1_load_1min,
            #'archive_deposit1_mem_swap': arch.get_archive_deposit1_mem_swap,
            #'archive_deposit2_load_1min': arch.get_archive_deposit2_load_1min,
            #'archive_deposit2_mem_swap': arch.get_archive_deposit2_mem_swap,
            #'archive_deposit3_load_1min': arch.get_archive_deposit3_load_1min,
            #'archive_deposit3_mem_swap': arch.get_archive_deposit3_mem_swap,
            #'archive_deposit4_load_1min': arch.get_archive_deposit4_load_1min,
            #'archive_deposit4_mem_swap': arch.get_archive_deposit4_mem_swap,
            #'archive_deposit5_load_1min': arch.get_archive_deposit5_load_1min,
            #'archive_deposit5_mem_swap': arch.get_archive_deposit5_mem_swap,
            #'archive_ingest1_up': arch.get_archive_ingest1_up,
            #'archive_ingest2_up': arch.get_archive_ingest2_up,
            #'archive_ingest3_up': arch.get_archive_ingest3_up,
            #'archive_ingest4_up': arch.get_archive_ingest4_up,
            #'archive_ingest5_up': arch.get_archive_ingest5_up,
            #'archive_rbq_up': arch.get_archive_rbq_up,
            #'archive_deposit1_up': arch.get_archive_deposit1_up,
            #'archive_deposit2_up': arch.get_archive_deposit2_up,
            #'archive_deposit3_up': arch.get_archive_deposit3_up,
            #'archive_deposit4_up': arch.get_archive_deposit4_up,
            #'archive_deposit5_up': arch.get_archive_deposit5_up,
            ##'users_jasmin': users.get_users_jasmin,
            ##'users_jasmin_login_active_today': users.get_users_jasmin_login_active_today,
            ##'users_jasmin_login_new_today': users.get_users_jasmin_login_new_today,
            ##'users_jasmin_login_new_week': users.get_users_jasmin_login_new_week,
            ##'users_jasmin_login_new_quarter': users.get_users_jasmin_login_new_quarter,
            #}

        self.service_status_list = {}
        # gauges
        self.met_funcs = {}
        for m in self.req_metrics:
            if 'lotus_' in m: 
                m_func = eval('lotus.get_{}'.format(m))
            elif 'archive_' in m:
                m_func = eval('arch.get_{}'.format(m))
            elif 'storage_' in m:
                m_func = eval('storage.get_{}'.format(m))
            self.met_funcs[m] = m_func
            gauge = pc.Gauge(m, m, registry=self.collector)
            self.service_status_list[m] = (gauge)


    def parse_metrics_config(self, fname):
        config = configparser.ConfigParser()
        config.read(fname)
        req_metrics = config.get('Metrics',self.metrics_group).split('\n')

        return req_metrics

    def create_view(self):
        for m in self.req_metrics:
            self.service_status_list[m].set(self.met_funcs[m]())

        return pc.generate_latest(registry=self.collector)

