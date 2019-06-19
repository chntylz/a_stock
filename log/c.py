#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import a
import b
from common import Log

log = Log(__name__).getlog()
log.info("I am c.py")
log.info("c=%d", 10)
log.info(10)