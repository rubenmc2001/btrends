# -*- encoding: utf-8 -*-
"""
Copyright (c) 2019 - present AppSeed.us
"""

from django.db import models
from django.contrib.auth.models import User

class CO2(models.Model):
    date = models.DateField()
    average = models.FloatField()

    class Meta:
        ordering = ('date',)


