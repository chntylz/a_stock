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

df=df.fillna(0)
df=round(df,2)
df['total_loan'] = 0
df['total_loan']  = df.st_loan+ df.interest_payable \
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
    list.append([df.stock_name[0], 'total_assets', 'total_assets_pct', 'result'])
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
    list.append([df.stock_name[0], 'total_assets', 'total_liab', 'asset_liab_ratio_x', 'result'])
    for i in range(df_len):
        if debug:
            print('record_date=%s, i=%d, total_assets=%f, total_liab=%f, asset_liab_ratio_x=%f, '\
                    %(df.record_date[i], i, df.total_assets[i]/y_unit, \
                    df.total_liab[i]/y_unit, df.asset_liab_ratio_x[i]))
        list.append([df.record_date[i], df.total_assets[i]/y_unit, df.total_liab[i]/y_unit,\
                df.asset_liab_ratio_x[i], df.asset_liab_ratio_x[i] < 60 ])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret

def income_analysis_loan(df):
    y_unit=10000*10000
    df_len=len(df)
    #youxifuzhai > huobijijin  
    #fuzhai vs zichan
    i = 0
    list = []
    list.append([df.stock_name[0], 'currency_funds', 'st_loan', \
                'interest_payable', 'noncurrent_liab_due_in1y', \
                'lt_loan', 'bond_payable', 'lt_payable', 'total_loan','result'])
    for i in range(df_len):
        if debug:
            print('record_date=%s, i=%d, currency_funds=%f, st_loan=%f, interest_payable=%f, \
                noncurrent_liab_due_in1y=%f, lt_loan=%f, bond_payable=%f, lt_payable=%f, \
                total_loan=%f '\
                %(df.record_date[i], i, df.currency_funds[i]/y_unit, df.st_loan[i]/y_unit, \
                df.interest_payable[i]/y_unit, df.noncurrent_liab_due_in1y[i]/y_unit, \
                df.lt_loan[i]/y_unit, df.bond_payable[i]/y_unit, df.lt_payable[i]/y_unit, \
                df.total_loan[i]/y_unit))
        list.append([df.record_date[i], df.currency_funds[i]/y_unit, df.st_loan[i]/y_unit, \
                df.interest_payable[i]/y_unit, df.noncurrent_liab_due_in1y[i]/y_unit, \
                df.lt_loan[i]/y_unit, df.bond_payable[i]/y_unit, df.lt_payable[i]/y_unit, \
                df.total_loan[i]/y_unit, df.currency_funds[i] > df.total_loan[i] ])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret

def income_analysis_payable_receivable(df):
    y_unit=10000*10000
    df_len=len(df)
    #
    # yingfuyushou vs yingshouyufu
    i = 0
    list = []
    list.append([df.stock_name[0], 'total_assets', 'bill_payable', 'accounts_payable', \
            'pre_receivable', 'total_payable',\
            'bills_receivable', 'account_receivable', 'pre_payment', 'total_receivable', \
            'payable-receivable', 'reveivable/total_assets'\
            ])
    for i in range(df_len):
        total_payable =  df.bill_payable[i] + df.accounts_payable[i] + df.pre_receivable[i]
        total_receivable = df.bills_receivable[i] + df.account_receivable[i] + df.pre_payment[i]
        list.append([df.record_date[i], \
            df.total_assets[i]/y_unit, df.bill_payable[i]/y_unit, df.accounts_payable[i]/y_unit,\
            df.pre_receivable[i]/y_unit, total_payable/y_unit, \
            df.bills_receivable[i]/y_unit,df.account_receivable[i]/y_unit,df.pre_payment[i]/y_unit,\
            total_receivable/y_unit,\
            (total_payable - total_receivable)/y_unit,\
            total_receivable / df.total_assets[i] * 100 \
            ])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret


def income_analysis_fixed_assets(df):
    y_unit=10000*10000
    df_len=len(df)
    #gudingzichan  < 40%
    #
    i = 0
    list = []
    list.append([df.stock_name[0],'fixed_asset_sum', 'construction_in_process_sum',\
        'project_goods_and_material', 'total_fixed', 'total_assets', \
        'total_fixed/total_assets', 'result'])
    for i in range(df_len):
        total_fixed = df.fixed_asset_sum[i] + df.construction_in_process_sum[i] \
            + df.project_goods_and_material[i]
        list.append([df.record_date[i], df.fixed_asset_sum[i]/y_unit, \
            df.construction_in_process_sum[i]/y_unit,\
            df.project_goods_and_material[i]/y_unit,\
            total_fixed/y_unit, df.total_assets[i]/y_unit, \
            total_fixed/df.total_assets[i] * 100, \
            total_fixed/df.total_assets[i] * 100 < 40 \
            ])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret

def income_analysis_invest(df):
    y_unit=10000*10000
    df_len=len(df)
    #invest ratio  < 10%
    i = 0
    list = []
    list.append([df.stock_name[0],'tradable_fnncl_assets', 'saleable_finacial_assets',\
        'lt_equity_invest', 'invest_property', 'total_invest', 'total_assets', \
        'total_fixed/total_assets', 'result'])
    for i in range(df_len):
        total_invest = df.tradable_fnncl_assets[i] + df.saleable_finacial_assets[i] \
            + df.lt_equity_invest[i] + df.invest_property[i]
        list.append([df.record_date[i], df.tradable_fnncl_assets[i]/y_unit, \
            df.saleable_finacial_assets[i]/y_unit,\
            df.lt_equity_invest[i]/y_unit, df.invest_property[i]/y_unit, \
            total_invest/y_unit, df.total_assets[i]/y_unit, \
            total_invest/df.total_assets[i] * 100, \
            total_invest/df.total_assets[i] * 100 < 10 \
            ])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret


def income_analysis_roe(df):
    y_unit=10000*10000
    df_len=len(df)
    # 15% < roe  < 39%
    i = 0
    list = []
    list.append([df.stock_name[0], 'net_profit_atsopc_x', 'net_profit_atsopc_new_x',\
        'total_quity_atsopc', 'roe', 'result'])
    for i in range(df_len):
        roe = df.net_profit_atsopc_x[i] * 100 / df.total_quity_atsopc[i]
        list.append([df.record_date[i], df.net_profit_atsopc_x[i]/y_unit, \
            df.net_profit_atsopc_new_x[i], \
            df.total_quity_atsopc[i]/y_unit, \
            roe, \
            15 < roe and roe < 39 \
            ])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret


def income_analysis_revenue(df):
    y_unit=10000*10000
    df_len=len(df)
    # revenue_yoy > 10%
    # (cash_ratio = cash / revenue) > 100%
    i = 0
    list = []
    list.append([df.stock_name[0], 'total_revenue', 'total_revenue_yoy',\
        'cash_received_of_sales_service', 'cash_ratio', 'result'])
    for i in range(df_len):
        cash_ratio = df.cash_received_of_sales_service[i] * 100 / df.total_revenue_x[i]
        condi = df.total_revenue_new_x[i] > 0.10 and cash_ratio > 100
        list.append([df.record_date[i], df.total_revenue_x[i] / y_unit, \
            df.total_revenue_new_x[i], \
            df.cash_received_of_sales_service[i]/y_unit, \
            cash_ratio, \
            condi\
            ])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret

def fina_data_analysis(df):
    all_df = df
    group_by_stock_code_df=all_df.groupby('stock_code')
    for stock_code, group_df in group_by_stock_code_df:
        if stock_code != 'SZ002475':
            continue
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
        #if asset_pct and liab_pct and loan_pct:
        #    print(stock_code_new, stock_name, asset_pct, liab_pct)
        return group_df
    pass


fina_data_analysis(df)



