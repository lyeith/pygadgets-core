#! <venv>/bin python3.8
# -*- coding: utf-8 -*-
"""
Created on 25/6/20 11:10 pm

@author: David Wong
"""


def truthy(v):
    if v:
        if v == 'None' or v == '0000-00-00 00:00:00':
            return False
        return True

    return False