# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from django.urls import path, re_path, include
from apps.home import views

urlpatterns = [
   
    path('', views.chart, name='chart'),# chart route
    path('home', views.index, name='home'),
    path('historial.html', views.historial_view, name='historial'),


    # Matches any html file
    re_path(r'^.*\.*', views.pages, name='pages'),

]
