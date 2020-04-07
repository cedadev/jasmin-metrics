from .metrics import MetricsView
from django.http import HttpResponse


def index(request):
    return HttpResponse('Metrics Index')

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


