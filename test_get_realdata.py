#!/usr/bin/evn python

import tushare as ts
import time


def show_realdata():

    my_list=['300750','300552', '000401']
    while True: 
        my_price=[]
        for code in my_list: 
            df = ts.get_realtime_quotes(code);
            my_price.append(list(df['price'])); 
        
        print("  %s  " % (my_list))
        print("%s" % (my_price))
        time.sleep(2);




show_realdata()
