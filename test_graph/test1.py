#!/usr/bin/python
# coding: UTF-8

import numpy as np
import matplotlib as mpl
mpl.use('Agg')


import matplotlib.pyplot as plt

x = np.range(0, 5, 0.1) # 50 x-axis points
y = sin(x) # y = sin(x)
plt.plot(x, y) 
plt.show()
