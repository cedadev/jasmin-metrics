from flask import Flask, Response
import prometheus_client as pc
import influxdb
import configparser
from archive_metrics import arhive_metrics
from lotus_metrics import lotus_metrics
from managed_cloud_metrics import mc_metrics
from unmanaged_cloud_metrics import umc_metrics
from network_metrics import network_metrics
from storage_metrics import storage_metrics
from tape_metrics import tape_metrics
from users_metrics import users_metrics
from power_metrics import power_metrics


def get_total_storage():
    return 1

class FlaskPrometheusView:
    '''Make a view for each test to be executed.  Express this view as a class
    in order to maintain state information.
    '''

    def __init__(self, service_status_list, req_metrics):
        '''Initialise, and take list of prometheus metrics
        '''
        self.service_status_list = service_status_list
        self.req_metrics = req_metrics
        lotus = lotus_metrics()
        storage = storage_metrics()
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
            'lotus_core_hours': lotus.get_lotus_core_hours,
            'lotus_util': lotus.get_lotus_util,
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
