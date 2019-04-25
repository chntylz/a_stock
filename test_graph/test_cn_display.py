#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import numpy as np


import matplotlib.pyplot as plt

import matplotlib
print matplotlib.matplotlib_fname()


plt.plot((1,2,3),(4,3,-1))
plt.xlabel(u'横坐标')
plt.ylabel(u'纵坐标')
plt.show()

plt.savefig("/home/aaron/aaron/test_graph/test_cn_display.jpg") 
