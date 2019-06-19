#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Sun May 02 11:08:16 2019

@author: aaron
"""

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create a file handler

handler = logging.FileHandler('hello.log')
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger

logger.addHandler(handler)

logger.info('Hello baby')


logger.info('Start reading database')
# read database here

records = {'john': 55, 'tom': 66}
logger.debug('Records: %s', records)
logger.info('Updating records ...')
# update records here

logger.info('Finish updating records')
