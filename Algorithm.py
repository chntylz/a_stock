#!/usr/bin/env python  
# -*- coding: utf-8 -*-
import os
import re

def get_c(s):
    if(len(s) < 1):
        return 0
    
    return s[-1]

def aaron_ref(s, n):
    if(len(s) < n):
        return 0
    
    return s[-(n+1)]

def aaron_cross(s1, s2):
    
    if(len(s1) < 2 or len(s2) < 2):
        return False

    if s1[-1] > s2[-1]:
        cond_1 = True
    else:
        cond_1 = False
    
    if s1[-2] < s2[-2]:
        cond_2 = True
    else:
        cond_2 = False
    
    return cond_1 and cond_2

