from flask import Flask, Response
import prometheus_client as pc
import influxdb
import configparser
from scripts.archive_metrics import ArchiveMetrics
from scripts.lotus_metrics import LotusMetrics
from scripts.managed_cloud_metrics import MCMetrics
from scripts.unmanaged_cloud_metrics import UMCMetrics
from scripts.network_metrics import NetworkMetrics
from scripts.storage_metrics import StorageMetrics
from scripts.tape_metrics import TapeMetrics
from scripts.users_metrics import UsersMetrics
from scripts.power_metrics import PowerMetrics


class FlaskPrometheusView:
    '''Make a view for each test to be executed.  Express this view as a class
    in order to maintain state information.
    '''

    def __init__(self, service_status_list, req_metrics):
        '''Initialise, and take list of prometheus metrics
        '''
        self.service_status_list = service_status_list
        self.req_metrics = req_metrics
        lotus = LotusMetrics()
        storage = StorageMetrics()
        mc = mc_metrics()

        # define dictionary of calculation functions
        self.met_funcs = {
            'storage_total': storage.get_storage_total,
            'storage_used': storage.get_storage_used,
            'storage_com': storage.get_storage_com,
            'storage_pfs_total': storage.get_storage_pfs_total,
            'storage_pfs_used': storage.get_storage_pfs_used,
            'storage_sof_total': storage.get_storage_sof_total,
            'storage_sof_used': storage.get_storage_sof_used,
            'storage_el_total': storage.get_storage_el_total,
            'storage_el_used': storage.get_storage_el_used,
            'lotus_cores_count': lotus.get_lotus_cores_count,
            'lotus_hosts_count': lotus.get_lotus_hosts_count,
            'lotus_mem_total': lotus.get_lotus_mem_total,
            'lotus_tbmonth_in': lotus.get_lotus_tbmonth_in,
            'lotus_tbmonth_out': lotus.get_lotus_tbmonth_out,
            'lotus_network_in': lotus.get_lotus_network_in,
            'lotus_network_out': lotus.get_lotus_network_out,
            'lotus_core_hours_day': lotus.get_lotus_core_hours_day,
            'lotus_core_hours_3day': lotus.get_lotus_core_hours_3day,
            'lotus_core_hours_week': lotus.get_lotus_core_hours_week,
            'lotus_core_hours_month': lotus.get_lotus_core_hours_month,
            'lotus_core_hours_avg_day': lotus.get_lotus_core_hours_avg_day,
            'lotus_core_hours_avg_3day': lotus.get_lotus_core_hours_avg_3day,
            'lotus_core_hours_avg_week': lotus.get_lotus_core_hours_avg_week,
            'lotus_core_hours_avg_month': lotus.get_lotus_core_hours_avg_month,
            'lotus_util_day': lotus.get_lotus_util_day,
            'lotus_util_3day': lotus.get_lotus_util_3day,
            'lotus_util_week': lotus.get_lotus_util_week,
            'lotus_util_month': lotus.get_lotus_util_month,
            'lotus_job_proc_min_day': lotus.get_lotus_job_proc_min_day,
            'lotus_job_proc_min_3day': lotus.get_lotus_job_proc_min_3day,
            'lotus_job_proc_min_week': lotus.get_lotus_job_proc_min_week,
            'lotus_job_proc_min_month': lotus.get_lotus_job_proc_min_month,
            'lotus_job_proc_avg_day': lotus.get_lotus_job_proc_avg_day,
            'lotus_job_proc_avg_3day': lotus.get_lotus_job_proc_avg_3day,
            'lotus_job_proc_avg_week': lotus.get_lotus_job_proc_avg_week,
            'lotus_job_proc_avg_month': lotus.get_lotus_job_proc_avg_month,
            'lotus_job_proc_max_day': lotus.get_lotus_job_proc_max_day,
            'lotus_job_proc_max_3day': lotus.get_lotus_job_proc_max_3day,
            'lotus_job_proc_max_week': lotus.get_lotus_job_proc_max_week,
            'lotus_job_proc_max_month': lotus.get_lotus_job_proc_max_month,
            'lotus_job_count_finished_day': lotus.get_lotus_job_count_finished_day,
            'lotus_job_count_finished_3day': lotus.get_lotus_job_count_finished_3day,
            'lotus_job_count_finished_week': lotus.get_lotus_job_count_finished_week,
            'lotus_job_count_finished_month': lotus.get_lotus_job_count_finished_month,
            'lotus_job_count_started_day': lotus.get_lotus_job_count_started_day,
            'lotus_job_count_started_3day': lotus.get_lotus_job_count_started_3day,
            'lotus_job_count_started_week': lotus.get_lotus_job_count_started_week,
            'lotus_job_count_started_month': lotus.get_lotus_job_count_started_month,
            'lotus_job_count_submitted_day': lotus.get_lotus_job_count_submitted_day,
            'lotus_job_count_submitted_3day': lotus.get_lotus_job_count_submitted_3day,
            'lotus_job_count_submitted_week': lotus.get_lotus_job_count_submitted_week,
            'lotus_job_count_submitted_month': lotus.get_lotus_job_count_submitted_month,
            'lotus_job_count_running_day': lotus.get_lotus_job_count_running_day,
            'lotus_job_count_running_3day': lotus.get_lotus_job_count_running_3day,
            'lotus_job_count_running_week': lotus.get_lotus_job_count_running_week,
            'lotus_job_count_running_month': lotus.get_lotus_job_count_running_month,
            'lotus_expansion_factor': lotus.get_lotus_expansion_factor,
            'lotus_wait_dur_tot': lotus.get_lotus_wait_dur_tot,
            'lotus_wait_dur_avg': lotus.get_lotus_wait_dur_avg,
            'lotus_wall_dur_tot': lotus.get_lotus_wall_dur_tot,
            'lotus_wall_dur_avg': lotus.get_lotus_wall_dur_avg,
            'openstack_vms_count': mc.get_openstack_vms_count,
            'openstack_vms_cpus_quota': mc.get_openstack_vms_cpus_quota,
            'openstack_vms_cpus_used': mc.get_openstack_vms_cpus_used,
            'openstack_vms_ram_quota': mc.get_openstack_vms_ram_quota,
            'openstack_vms_ram_used': mc.get_openstack_vms_ram_used,
            'openstack_vms_storage_quota': mc.get_openstack_vms_storage_quota,
            'openstack_vms_storage_used': mc.get_openstack_vms_storage_used,
       }


    def __call__(self):
        '''Use call method to make instances of this class a callable
        function to which a Flask view can be attached
        '''


        for m in self.req_metrics['gauge']:
            self.service_status_list[m].set(self.met_funcs[m]())

        return Response(pc.generate_latest(),
                        mimetype='text/plain; charset=utf-8')




def parse_metrics_config(fname='./metrics.ini'):
    req_metrics = {}
    config = configparser.ConfigParser()
    config.read(fname)
    req_metrics['gauge'] = config.get('Metrics','gauge').split('\n')

    return req_metrics

def flask_app_factory():

    app = Flask(__name__)
    collector = pc.CollectorRegistry()

    req_metrics = parse_metrics_config()
    service_status_list = {}
    # gauges
    for m in req_metrics['gauge']:
        gauge = pc.Gauge(m, m)
        service_status_list[m] = (gauge)

    flask_view = FlaskPrometheusView(service_status_list, 
                                    req_metrics)

    path = '/metrics/'
    app.add_url_rule(path, 'metrics', flask_view)

    return app


if __name__ == '__main__':
    app = flask_app_factory()
    app.run('0.0.0.0', '8091')
    #parse_metrics_config()
