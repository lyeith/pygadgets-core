#! <venv>/bin python3.8
# -*- coding: utf-8 -*-
"""
Created on 5/11/20 1:24 pm

@author: David Wong
"""
import simplejson as json
from bson import json_util


def dumps(val):
    return json.dumps(val, default=json_util.default, use_decimal=True)
