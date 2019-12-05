from flask import Flask, Response
import prometheus_client as pc
import influxdb
import configparser
from metrics import *


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

        # define dictionary of calculation functions
        self.met_funcs = {
            'storage_total': get_storage_total,
            'storage_used': get_storage_used,
            'storage_pfs_total': get_storage_pfs_total,
            'storage_pfs_used': get_storage_pfs_used,
            'storage_sof_total': get_storage_sof_total,
            'storage_sof_used': get_storage_sof_used,
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

    req_metrics = parse_metrics_config()
    service_status_list = {}
    # gauges
    for m in req_metrics['gauge']:
        gauge = pc.Gauge(m, m)
        service_status_list[m] = (gauge)

    flask_view = FlaskPrometheusView(service_status_list, req_metrics)

    path = '/metrics/'
    app.add_url_rule(path, 'metrics', flask_view)

    return app


if __name__ == '__main__':
    app = flask_app_factory()
    app.run('0.0.0.0', '8091')
    #parse_metrics_config()