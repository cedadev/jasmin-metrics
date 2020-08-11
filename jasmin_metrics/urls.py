"""jasmin_metrics URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from . import views 
 


from . import views

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', views.index),
    path('metrics/<str:period>/', views.prom_metrics),
    path('metrics/<str:period>/list', views.metrics_list),
    path('reports/volume', views.volume_report),
    path('reports/gws_users', views.gws_users_report),
    path('reports/gws_scanner', views.gws_scanner_report),
]
