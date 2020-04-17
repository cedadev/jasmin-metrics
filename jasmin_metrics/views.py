from .metrics import MetricsView
from django.http import HttpResponse
from django.shortcuts import render


def index(request):
    return render(request, 'jasmin_metrics/index.html')

def metrics_list(request, period):
    mv = MetricsView(period)
    metrics = mv.req_metrics
    return render(request, 'jasmin_metrics/metrics_list.html', {'period':period, 'metrics': metrics})

def dashboards(request):
    return render(request, 'jasmin_metrics/dashboards.html')

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


