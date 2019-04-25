#!/usr/bin/python
# coding: UTF-8
'''This script parse stock info'''

'''
现在试一下简单的分析,即设定一个策略：以20日线为标准,当前股价低于20日线的时候就卖出,高于20日线的时候就买入。

然后计算一下这个策略的效果。
'''

import tushare as ts
def parse(code_list):
    '''process stock'''
    is_buy = 0
    buy_val = []
    buy_date = []
    sell_val = []
    sell_date = []
    df = ts.get_hist_data(STOCK)
    ma20 = df[u'ma20']
    close = df[u'close']
    rate = 1.0
    idx = len(ma20)

    while idx > 0:
        idx -= 1
        close_val = close[idx]
        ma20_val = ma20[idx]
        if close_val > ma20_val:
            if is_buy == 0:
                is_buy = 1
                buy_val.append(close_val)
                buy_date.append(close.keys()[idx])
        elif close_val < ma20_val:
            if is_buy == 1:
                is_buy = 0
                sell_val.append(close_val)
                sell_date.append(close.keys()[idx])
   
    print 'stock number: %s' %STOCK
    print 'buy count : %d' %len(buy_val)
    print 'sell count : %d' %len(sell_val)
    
    for i in range(len(sell_val)):
        rate = rate * (sell_val[i] * (1-0.002) / buy_val[i])
        print 'buy date : %s, buy price : %.2f' %(buy_date[i], buy_val[i])
        print 'sell date: %s, sell price: %.2f' %(sell_date[i], sell_val[i])
        print 'rate: %.2f' % rate


if __name__ == '__main__':
    STOCK = '600000' ##浦发银行
    parse(STOCK)
