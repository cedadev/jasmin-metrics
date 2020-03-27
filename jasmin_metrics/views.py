from .metrics import MetricsView
from django.http import HttpResponse


def index(request):
    return HttpResponse('Metrics Index')

def arch_metrics(request):
    am = MetricsView('jasmin_metrics/arch_metrics.ini')
    return HttpResponse(am.create_view(), content_type='text/plain')

def freq_metrics(request):
    fm = MetricsView('jasmin_metrics/metrics.ini')
    return HttpResponse(fm.create_view(), content_type='text/plain')

def daily_metrics(request):
    dm = MetricsView('jasmin_metrics/daily_metrics.ini')
    return HttpResponse(dm.create_view(), content_type='text/plain')

def weekly_metrics(request):
    wm = MetricsView('jasmin_metrics/weekly_metrics.ini')
    return HttpResponse(wm.create_view(), content_type='text/plain')

def monthly_metrics(request):
    mm = MetricsView('jasmin_metrics/monthly_metrics.ini')
    return HttpResponse(mm.create_view(), content_type='text/plain')


