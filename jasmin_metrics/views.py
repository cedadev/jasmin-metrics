from .metrics import MetricsView
from .scripts.reports import *
from django.http import HttpResponse
from django.shortcuts import render


def index(request):
    return render(request, 'jasmin_metrics/index.html')

def metrics_list(request, period):
    mv = MetricsView(period)
    metrics = mv.req_metrics
    return render(request, 'jasmin_metrics/metrics_list.html', {'period':period, 'metrics': metrics})


def prom_metrics(request, period):
    mv = MetricsView(period)
    return HttpResponse(mv.create_view(), content_type='text/plain')

def arch_metrics(request):
    am = MetricsView('archive')
    return HttpResponse(am.create_view(), content_type='text/plain')

def freq_metrics(request):
    fm = MetricsView('freq')
    return HttpResponse(fm.create_view(), content_type='text/plain')

def daily_metrics(request):
    dm = MetricsView('daily')
    return HttpResponse(dm.create_view(), content_type='text/plain')

def weekly_metrics(request):
    wm = MetricsView('weekly')
    return HttpResponse(wm.create_view(), content_type='text/plain')

def monthly_metrics(request):
    mm = MetricsView('monthly')
    return HttpResponse(mm.create_view(), content_type='text/plain')

def volume_report(request):
    vr = VolumeReport()
    return vr.create_view()

def gws_users_report(request):
    gr = GWSUsersReport()
    return gr.create_view()

def gws_list_report(request):
    gl = GWSList()
    return gl.create_view()

def gws_scanner_report(request):
    gm = GWSScannerInput()
    return gm.create_view()