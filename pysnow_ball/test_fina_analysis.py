import pysnowball as ball
from pysnowball import utls
from comm_interface import *
import pandas as pd


from HData_xq_fina import *
from HData_xq_holder import *
from HData_xq_income import *
from HData_xq_balance  import *

from xq_main_fina import *

#funcat
from funcat import *
from funcat.data.aaron_backend import AaronDataBackend
set_data_backend(AaronDataBackend())

token=get_cookie()
ball.set_token(token)

debug = 1

hdata_fina     = HData_xq_fina("usr","usr")
hdata_income   = HData_xq_income("usr","usr")
hdata_balance  = HData_xq_balance("usr","usr")
hdata_cashflow = HData_xq_cashflow("usr","usr")

df_fina     = hdata_fina.get_data_from_hdata()
df_income   = hdata_income.get_data_from_hdata()
df_balance  = hdata_balance.get_data_from_hdata()
df_cashflow = hdata_cashflow.get_data_from_hdata()

df_fina = df_fina.sort_values('record_date', ascending=0)
df_fina = df_fina.reset_index(drop=True)

df_income = df_income.sort_values('record_date', ascending=0)
df_income = df_income.reset_index(drop=True)

df_balance = df_balance.sort_values('record_date', ascending=0)
df_balance = df_balance.reset_index(drop=True)

df_cashflow = df_cashflow.sort_values('record_date', ascending=0)
df_cashflow = df_cashflow.reset_index(drop=True)




df_y_fina       = df_fina[df_fina['report_name'].str.contains('年报')]
df_y_income     = df_income[df_income['report_name'].str.contains('年报')]
df_y_balance    = df_balance[df_balance['report_name'].str.contains('年报')]
df_y_cashflow   = df_cashflow[df_cashflow['report_name'].str.contains('年报')]

df_y_fina.iloc[:, :5].head(1)    
df_y_income.iloc[:, :5].head(1)   
df_y_balance.iloc[:, :5].head(1)  
df_y_cashflow.iloc[:, :5].head(1) 



df_tmp = pd.merge(df_y_fina, df_y_income, how='outer', \
        on=['record_date', 'stock_code', 'report_name'])
df_tmp = pd.merge(df_tmp, df_y_balance, how='outer', \
        on=['record_date', 'stock_code', 'report_name'])
df = df_y_income_balance = pd.merge(df_tmp, df_y_cashflow, how='outer', \
        on=['record_date', 'stock_code', 'report_name'])


df.total_loan = 0
df.total_loan = df.st_loan+ df.interest_payable \
    + df.noncurrent_liab_due_in1y + df.lt_loan \
    + df.bond_payable + df.lt_payable

len(df_y_fina)
len(df_y_income)  
len(df_y_balance)
len(df_y_cashflow)
len(df_y_income_balance)

df_y_fina[df_y_fina['stock_code'] == 'SZ002475']
df_y_income[df_y_income['stock_code'] == 'SZ002475']
df_y_balance[df_y_balance['stock_code'] == 'SZ002475']
df_y_cashflow[df_y_cashflow['stock_code'] == 'SZ002475']
df_y_income_balance[df_y_income_balance['stock_code'] == 'SZ002475']


df_y_balance[df_y_balance.stock_code=='SH600519'].total_assets



def income_analysis_assets(df):
    y_unit=10000*10000
    df_len=len(df)
    #zong zi chan zengzhanglv  > 20%
    #total_assets_new
    i = 0
    list = []
    list.append([df.stock_name[0], 'total_assests', 'total_assests_pct', 'result'])
    for i in range(df_len):
        if debug:
            print('record_date=%s, i=%d, total_assets=%f, total_assets_new=%f'\
                    %(df.record_date[i], i, df.total_assets[i]/y_unit, df.total_assets_new[i]))
        list.append([df.record_date[i], df.total_assets[i]/y_unit, \
                df.total_assets_new[i], df.total_assets_new[i] >= 0.2])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret

def income_analysis_liab(df):
    y_unit=10000*10000
    df_len=len(df)
    #zong zi chan fuzhailv  < 60%
    #asset_liab_ratio_x
    i = 0
    list = []
    list.append([df.stock_name[0], 'total_assests', 'total_liab', 'asset_liab_ratio_x', 'result'])
    for i in range(df_len):
        if debug:
            print('record_date=%s, i=%d, total_assets, total_liab=%f, asset_liab_ratio_x=%f, '\
                    %(df.record_date[i], i, df.total_assets[i]/y_unit, \
                    df.total_liab[i]/y_unit, df.asset_liab_ratio_x[i]))
        list.append([df.record_date[i], df.total_assets[i]/y_unit, df.total_liab[i]/y_unit,\
                df.asset_liab_ratio_x[i], df.asset_liab_ratio_x[i] < 60 ])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret

def income_analysis_loan(df):
    ret = False
    y_unit=10000*10000
    df_len=len(df)
    #youxifuzhai > huobijijin  
    #fuzhai vs zichan
    i = 0
    for i in range(df_len):
        if debug:
            print('record_date=%s, i=%d, currency_funds=%f, st_loan=%f, interest_payable=%f, \
                noncurrent_liab_due_in1y=%f, lt_loan=%f, bond_payable=%f, lt_payable=%f '\
                %(df.record_date[i], i, df.currency_funds[i], df.st_loan[i], \
                df.interest_payable[i], df.noncurrent_liab_due_in1y[i], \
                df.lt_loan[i], df.bond_payable[i], df.lt_payable[i]))
        t_loan = df.st_loan[i] + df.interest_payable[i] \
                    + df.noncurrent_liab_due_in1y[i] + df.lt_loan[i], \
                    + df.bond_payable[i] + df.lt_payable[i]
        if debug:
            print(t_loan)
        if (df.currency_funds[i] < t_loan[0]) :
            break
    if i == df_len-1 :
        if debug:
            print('record_date=%s, i=%d, currency_funds=%f, st_loan=%f, interest_payable=%f, \
                noncurrent_liab_due_in1y=%f, lt_loan=%f, bond_payable=%f, lt_payable=%f '\
                %(df.record_date[i], i, df.currency_funds[i], df.st_loan[i], \
                df.interest_payable[i], df.noncurrent_liab_due_in1y[i], \
                df.lt_loan[i], df.bond_payable[i], df.lt_payable[i]))
        ret = True 
    return ret




def fina_data_analysis(df):
    all_df = df
    group_by_stock_code_df=all_df.groupby('stock_code')
    for stock_code, group_df in group_by_stock_code_df:
        #if stock_code != 'SZ002475':
        #    continue
        group_df = group_df.reset_index(drop=True)
        if debug:
            print(stock_code)
            print(group_df.head(1))
        #get stock_cname
        stock_code_new = stock_code[2:] 
        stock_name = symbol(stock_code_new)
        pos_s=stock_name.rfind('[')
        pos_e=stock_name.rfind(']')
        stock_name=stock_name[pos_s+1: pos_e]
        group_df.insert(1, 'stock_name' , stock_name, allow_duplicates=False)
        asset_pct = income_analysis_assets(group_df)
        liab_pct  = income_analysis_liab(group_df)
        loan_pct  = income_analysis_loan(group_df)
        if asset_pct and liab_pct and loan_pct:
            print(stock_code_new, stock_name, asset_pct, liab_pct)
        #return group_df
    pass


fina_data_analysis(df)



