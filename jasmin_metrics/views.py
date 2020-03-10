from django.http import HttpResponse

from .metrics import metrics_app

app = metrics_app()

def index(request):
    return HttpResponse("Hello world. index")

def main_metrics(request):
    view = metrics_app('./metrics')
    resp = view.create_metrics_view()
    return HttpResponse(resp)
