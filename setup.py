#! <venv>/bin python3.6
# -*- coding: utf-8 -*-
"""
Created on Feb 01 14:11:28 2019

@author: david
"""
from setuptools import setup, find_namespace_packages

setup(
    name='pygadgets_core',
    version='0.1.164',
    author='David Wong',
    author_email='david.wong.jm@outlook.com',
    description='Useful Python Gadgets',
    long_description='Data Wrangling and Stuff',
    long_description_content_type='text/markdown',
    url='https://github.com/lyeith/pygadgets',
    packages=setuptools.find_namespace_packages(),
    install_requires=[
        'pymongo',
        'pymysql',
        'psycopg2-binary',
        'newrelic'
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent'
    ],
    python_requires='>=3.6'
)
