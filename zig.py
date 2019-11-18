# coding: utf-8 
"""
Created on Sat Jan 05 18:53:39 2019
http://www.pianshen.com/article/363258879/
@author: duanqs
"""
import numpy as np
import tushare as ts

import matplotlib.pyplot as plt
 
ZIG_STATE_START = 0
ZIG_STATE_RISE = 1
ZIG_STATE_FALL = 2


def zig(df):
    x = 0.35
    k = df["close"]
    d = df.index
    #print(k)
    #print(d)
    # 循环前的变量初始化
    # 端点 候选点 扫描点 端点列表 拐点线列表 趋势状态
    peer_i = 0
    candidate_i = None
    scan_i = 0
    peers = [0]
    z = np.zeros(len(k))
    state = ZIG_STATE_START
    while True:
        #print(peers)
        scan_i += 1
        if scan_i == len(k) - 1:
            # 扫描到尾部
            if candidate_i is None:
                peer_i = scan_i
                peers.append(peer_i)
            else:
                if state == ZIG_STATE_RISE:
                    if k[scan_i] >= k[candidate_i]:
                        peer_i = scan_i
                        peers.append(peer_i)
                    else:
                        peer_i = candidate_i
                        peers.append(peer_i)
                        peer_i = scan_i
                        peers.append(peer_i)
                elif state == ZIG_STATE_FALL:
                    if k[scan_i] <= k[candidate_i]:
                        peer_i = scan_i
                        peers.append(peer_i)
                    else:
                        peer_i = candidate_i
                        peers.append(peer_i)
                        peer_i = scan_i
                        peers.append(peer_i)
            break
 
        if state == ZIG_STATE_START:
            if k[scan_i] >= k[peer_i] * (1 + x):
                candidate_i = scan_i
                state = ZIG_STATE_RISE
            elif k[scan_i] <= k[peer_i] * (1 - x):
                candidate_i = scan_i
                state = ZIG_STATE_FALL
        elif state == ZIG_STATE_RISE:
            if k[scan_i] >= k[candidate_i]:
                candidate_i = scan_i
            elif k[scan_i] <= k[candidate_i]*(1-x):
                peer_i = candidate_i
                peers.append(peer_i)
                state = ZIG_STATE_FALL
                candidate_i = scan_i
        elif state == ZIG_STATE_FALL:
            if k[scan_i] <= k[candidate_i]:
                candidate_i = scan_i
            elif k[scan_i] >= k[candidate_i]*(1+x):
                peer_i = candidate_i
                peers.append(peer_i)
                state = ZIG_STATE_RISE
                candidate_i = scan_i
    
    #线性插值， 计算出zig的值            
    for i in range(len(peers) - 1):
        peer_start_i = peers[i]
        peer_end_i = peers[i+1]
        start_value = k[peer_start_i]
        end_value = k[peer_end_i]
        a = (end_value - start_value)/(peer_end_i - peer_start_i)# 斜率
        for j in range(peer_end_i - peer_start_i +1):
            z[j + peer_start_i] = start_value + a*j
    
    print(u'...转向点的阀值、个数、位置和日期...')        
    print(x, len(peers))
    print("peers:%s" % peers)
    dates = [d[i] for i in peers]
    print("dates:%s" % dates)
    print([k[i] for i in peers])
    #print(list(k))
    #print(list(z))
    
    return z, peers, d, k


