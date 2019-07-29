#!/usr/bin/evn python

from funcat import *
from funcat.data.aaron_backend import AaronDataBackend
set_data_backend(AaronDataBackend())

s_list=[['20190214', '603607'],
        ['20190109', '000860'],
        ['20181112', '603589'],
        ['20190107', '603589'],
        ['20190527', '603568'],
        ['20190716', '603477'],
        ['20190528', '603477'],
        ['20190213', '603477'],
        ['20190104', '603477'],
    ]

s_length=len(s_list)
delta_time=7
for i in range(0, s_length):
    T(s_list[i][0]); 
    S(s_list[i][1]);
    delta = (HHV(MAX(O, C), delta_time) -  LLV(MAX(O, C), delta_time))/HHV(MAX(O, C), delta_time)
    print('date:%s %s delta:%f'%(s_list[i][0], s_list[i][1], delta.value))
