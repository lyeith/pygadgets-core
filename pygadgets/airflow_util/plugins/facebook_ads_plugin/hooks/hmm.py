#! <venv>/bin python3.8
# -*- coding: utf-8 -*-
"""
Created on 21/10/20 1:45 pm

@author: David Wong
"""
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount


my_app_id = '3396366213760437'
my_app_secret = '332122d6f131b3a6329d1d07279baf13'
my_access_token = 'EAAwQZBiGyxbUBANWYkO29qZCxQIDsouqUNXLfPs6vZCwMkXKSdOTnY6COow7a7tCtpKaGzG4QZBKq17b0BxS2zQASdRkhujfsZCYYpZBqgUEVZAxQ324iWTZBWiAFCZAxEu54T6pt2yPXFGhSaX2dfsZC0vg06dhU6DJq9LZBiLwvvS0QZDZD'
FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
my_account = AdAccount('act_502604490299991')
campaigns = my_account.get_campaigns()