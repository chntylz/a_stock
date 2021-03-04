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


#1-1
def income_analysis_assets(df):
    y_unit=10000*10000
    df_len=len(df)
    #zong zi chan zengzhanglv  > 20%
    #total_assets_new
    i = 0
    list = []
    list.append([df.stock_name[0], '总资产', '总资产增长率', 'result'])
    '''
    list.append([df.stock_name[0], 'total_assets', 'total_assets_pct', 'result'])
    '''
    for i in range(df_len):
        if debug:
            print('record_date=%s, i=%d, total_assets=%f, total_assets_new=%f'\
                    %(df.record_date[i], i, df.total_assets[i]/y_unit, df.total_assets_new[i] * 100))
        list.append([df.record_date[i], df.total_assets[i]/y_unit, \
                df.total_assets_new[i] * 100 , df.total_assets_new[i] >= 0.2])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret

#1-2
def income_analysis_liab(df):
    y_unit=10000*10000
    df_len=len(df)
    #zong zi chan fuzhailv  < 60%
    #asset_liab_ratio_x
    i = 0
    list = []
    list.append([df.stock_name[0], '总资产', '总负债', '资产负债率', 'result'])
    '''
    list.append([df.stock_name[0], 'total_assets', 'total_liab', 'asset_liab_ratio_x', 'result'])
    '''
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

#1-3
def income_analysis_loan(df):
    y_unit=10000*10000
    df_len=len(df)
    #youxifuzhai > huobijijin  
    #fuzhai vs zichan
    i = 0
    list = []
    list.append([df.stock_name[0], '货币资金', '短期借款', \
                '应付利息', '一年内到期的非流动负债', \
                '长期借款', '应付债券', '长期应付款', '有息负债总额','result'])
    '''
    list.append([df.stock_name[0], 'currency_funds', 'st_loan', \
                'interest_payable', 'noncurrent_liab_due_in1y', \
                'lt_loan', 'bond_payable', 'lt_payable', 'total_loan','result'])
    '''
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

#1-4
def income_analysis_payable_receivable(df):
    y_unit=10000*10000
    df_len=len(df)
    #
    # yingfuyushou vs yingshouyufu
    i = 0
    list = []
    list.append([df.stock_name[0], '总资产', '应付票据', '应付账款', \
            '预收款项', '应付预收合计',\
            '应收票据', '应收账款', '预付款项', '应收预付合计', \
            '应付预收 - 应收预付', '应收账款/总资产', 'result'\
            ])
    ''' 
    list.append([df.stock_name[0], 'total_assets', 'bill_payable', 'accounts_payable', \
            'pre_receivable', 'total_payable',\
            'bills_receivable', 'account_receivable', 'pre_payment', 'total_receivable', \
            'payable-receivable', 'reveivable/total_assets', 'result'\
            ])
    ''' 
    for i in range(df_len):
        total_payable =  df.bill_payable[i] + df.accounts_payable[i] + df.pre_receivable[i]
        total_receivable = df.bills_receivable[i] + df.account_receivable[i] + df.pre_payment[i]
        recv_of_total_assets =  total_receivable / df.total_assets[i] * 100 
        list.append([df.record_date[i], \
            df.total_assets[i]/y_unit, df.bill_payable[i]/y_unit, df.accounts_payable[i]/y_unit,\
            df.pre_receivable[i]/y_unit, total_payable/y_unit, \
            df.bills_receivable[i]/y_unit,df.account_receivable[i]/y_unit,df.pre_payment[i]/y_unit,\
            total_receivable/y_unit,\
            (total_payable - total_receivable)/y_unit,\
            total_receivable / df.total_assets[i] * 100 ,\
            recv_of_total_assets < 20

            ])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret

#1-5
def income_analysis_fixed_assets(df):
    y_unit=10000*10000
    df_len=len(df)
    #gudingzichan  < 40%
    #
    i = 0
    list = []
    list.append([df.stock_name[0],'固定资产', '在建工程',\
        '工程物资', '固定资产合计', '总资产', \
        '固定资产/总资产', 'result'])
    '''
    list.append([df.stock_name[0],'fixed_asset_sum', 'construction_in_process_sum',\
        'project_goods_and_material', 'total_fixed', 'total_assets', \
        'total_fixed/total_assets', 'result'])
    '''
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

#1-6
def income_analysis_invest(df):
    y_unit=10000*10000
    df_len=len(df)
    #invest ratio  < 10%
    i = 0
    list = []
    list.append([df.stock_name[0],'以公允价值计量的资产', '可供出售的金融资产',\
        '长期股权投资', '投资性房地产', '总投资', '总资产', \
        '与主业无关的投资类资产占比', 'result'])
    '''
    list.append([df.stock_name[0],'tradable_fnncl_assets', 'saleable_finacial_assets',\
        'lt_equity_invest', 'invest_property', 'total_invest', 'total_assets', \
        'total_fixed/total_assets', 'result'])
    '''
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

#1-7
def income_analysis_roe(df):
    y_unit=10000*10000
    df_len=len(df)
    # 15% < roe  < 39%
    i = 0
    list = []
    list.append([df.stock_name[0], '归母净利润', '归母净利润增长率',\
        '归属母公司股东权益合计', '净资产收益率', 'result'])
    '''
    list.append([df.stock_name[0], 'net_profit_atsopc_x', 'net_profit_atsopc_new_x',\
        'total_quity_atsopc', 'roe', 'result'])
    '''
    for i in range(df_len):
        roe = df.net_profit_atsopc_x[i] * 100 / df.total_quity_atsopc[i]
        list.append([df.record_date[i], df.net_profit_atsopc_x[i]/y_unit, \
            df.net_profit_atsopc_new_x[i] * 100, \
            df.total_quity_atsopc[i]/y_unit, \
            roe, \
            15 < roe and roe < 39 \
            ])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret

#2-1
def income_analysis_revenue(df):
    y_unit=10000*10000
    df_len=len(df)
    # revenue_yoy > 10%
    # (cash_ratio = cash / revenue) > 100%
    i = 0
    list = []
    list.append([df.stock_name[0], '营业收入', '营业收入增长率',\
        '销售商品、提供劳务收到的现金', '现金占比', 'result'])
    '''
    list.append([df.stock_name[0], 'total_revenue', 'total_revenue_yoy',\
        'cash_received_of_sales_service', 'cash_ratio', 'result'])
    '''
    for i in range(df_len):
        cash_ratio = df.cash_received_of_sales_service[i] * 100 / df.total_revenue_x[i]
        condi = df.total_revenue_new_x[i] * 100 > 10 and cash_ratio > 100
        list.append([df.record_date[i], df.total_revenue_x[i] / y_unit, \
            df.total_revenue_new_x[i] * 100, \
            df.cash_received_of_sales_service[i]/y_unit, \
            cash_ratio, \
            condi\
            ])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret

#2-2
def income_analysis_gross(df):
    y_unit=10000*10000
    df_len=len(df)
    # gross_ratio > 40%
    i = 0
    list = []
    list.append([df.stock_name[0], '营业收入', '营业成本',\
        '毛利', '毛利率', 'result'])
    '''
    list.append([df.stock_name[0], 'total_revenue', 'operating_cost',\
        'gross', 'gross_ratio', 'result'])
    '''
    for i in range(df_len):
        gross =  df.total_revenue_x[i] - df.operating_cost[i] 
        gross_ratio = gross * 100 / df.total_revenue_x[i]
        condi = gross_ratio > 40
        list.append([df.record_date[i], df.total_revenue_x[i] / y_unit, \
            df.operating_cost[i]/y_unit, \
            gross / y_unit, \
            gross_ratio, \
            condi\
            ])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret


#2-3
def income_analysis_costfee(df):
    y_unit=10000*10000
    df_len=len(df)
    # costfee_ratio > 40%
    i = 0
    list = []
    list.append([df.stock_name[0], '营业收入', '营业成本',\
        '毛利', '销售费用', '管理费用', '财务费用', '研发费用', '四费合计', \
        '费用率', '毛利率', '费用率/毛利率', 'result'])
    '''
    list.append([df.stock_name[0], 'total_revenue', 'operating_cost',\
        'gross', 'sales_fee', 'manage_fee', 'financing_expenses', 'rad_cost', 'total_4fee', \
        'costfee_p', 'gross_ratio', 'costfee_ratio', 'result'])
    '''
    for i in range(df_len):
        gross =  df.total_revenue_x[i] - df.operating_cost[i] 
        gross_ratio = gross * 100 / df.total_revenue_x[i]
        total_4fee = df.sales_fee[i] + df.manage_fee[i] + df.financing_expenses[i] + df.rad_cost[i]
        costfee_p = total_4fee * 100 / df.total_revenue_x[i]
        costfee_ratio = costfee_p * 100 / gross_ratio
        condi = gross_ratio > 40 and costfee_ratio < 60
        list.append([df.record_date[i], df.total_revenue_x[i] / y_unit, \
            df.operating_cost[i]/y_unit, \
            gross / y_unit, \
            df.sales_fee[i] / y_unit, \
            df.manage_fee[i]/ y_unit, \
            df.financing_expenses[i]/ y_unit, \
            df.rad_cost[i]/ y_unit, \
            total_4fee / y_unit, costfee_p, \
            gross_ratio, costfee_ratio,   \
            condi])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret

#2-4
def income_analysis_main_profit(df):
    y_unit=10000*10000
    df_len=len(df)
    # costfee_ratio > 40%
    i = 0
    list = []
    list.append([df.stock_name[0], '营业收入', '营业成本',\
        '营业税金及附加', \
        '销售费用', '管理费用', '财务费用', '研发费用', '四费合计', \
        '利润总额', \
        '投资收益', '公允价值变动损益', '资产减值', '营业外收入', \
        '营业外支出', '信用减值损失', '其他收益', '资产处置收益', \
        '所有其他收益', '主营利润', \
        '主营利润率', '主营利润/利润总额', 'result'])
 
    '''
    list.append([df.stock_name[0], 'total_revenue', 'operating_cost',\
        'operating_taxes_and_surcharge', \
        'sales_fee', 'manage_fee', 'financing_expenses', 'rad_cost', 'total_4fee', \
        'total_profit', \
        'invest_income', 'income_from_chg_in_fv', 'asset_impairment_loss', 'non_operating_income', \
        'non_operating_payout', 'credit_impairment_loss', 'other_income', 'asset_disposal_income', \
        'all_other_income', 'main_profit', \
        'main_profit_of_total_revenue', 'main_profit_of_total_profit', 'result'])
    '''
    for i in range(df_len):
        total_4fee = (df.sales_fee[i] + df.manage_fee[i] + \
                df.financing_expenses[i] + df.rad_cost[i])
        all_other_income = (df.invest_income[i] + df.income_from_chg_in_fv[i] - \
                df.asset_impairment_loss[i]+df.non_operating_income[i] - \
                df.non_operating_payout[i] - df.credit_impairment_loss[i] + \
                df.other_income[i] + df.asset_disposal_income[i])
        total_profit = df.profit_total_amt[i]
        main_profit = (total_profit - all_other_income)  
        main_profit_of_total_revenue = main_profit * 100 / df.total_revenue_x[i] 
        main_profit_of_total_profit = main_profit * 100 / total_profit
        condi = main_profit_of_total_profit > 80
        list.append([df.record_date[i], df.total_revenue_x[i] / y_unit, \
            df.operating_cost[i]/y_unit, \
            df.operating_taxes_and_surcharge[i] / y_unit, \
            df.sales_fee[i] / y_unit, \
            df.manage_fee[i]/ y_unit, \
            df.financing_expenses[i]/ y_unit, \
            df.rad_cost[i]/ y_unit, \
            total_4fee / y_unit, \
            total_profit / y_unit, \
            df.invest_income[i]/ y_unit, df.income_from_chg_in_fv[i]/ y_unit, \
            df.asset_impairment_loss[i]/ y_unit, df.non_operating_income[i]/ y_unit, \
            df.non_operating_payout[i]/ y_unit, df.credit_impairment_loss[i]/ y_unit, \
            df.other_income[i]/ y_unit, df.asset_disposal_income[i]/ y_unit,  \
            all_other_income/ y_unit, main_profit/ y_unit, \
            main_profit_of_total_revenue, main_profit_of_total_profit, \
            condi])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret


#2-5
def income_analysis_net_profit(df):
    y_unit=10000*10000
    df_len=len(df)
    # ncf_ratio > 100%
    i = 0
    list = []
    list.append([df.stock_name[0], '经营活动产生的现金流量净额', '经营活动产生的现金流量净额增长率',\
        '净利润', '净利润现金比率', 'result'])
    '''
    list.append([df.stock_name[0], 'ncf_from_oa', 'ncf_from_oa_yoy',\
        'net_profit', 'net_profit_of_ncf_from_oa', 'result'])
    '''
    total_net_profit = 0
    total_ncf_from_oa = 0
    for i in range(df_len):
        total_ncf_from_oa += df.ncf_from_oa[i] 
        total_net_profit  += df.net_profit[i]
        net_profit_of_ncf_from_oa  = df.ncf_from_oa[i]  * 100/  df.net_profit[i]
        condi = net_profit_of_ncf_from_oa > 100 
        list.append([df.record_date[i], \
            df.ncf_from_oa[i]/ y_unit, \
            df.ncf_from_oa_new[i] * 100, \
            df.net_profit[i] / y_unit,\
            net_profit_of_ncf_from_oa ,\
            condi])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret

#3-1
#cash flow

#3-2
def income_analysis_paid_assets(df):
    y_unit=10000*10000
    df_len=len(df)
    # paid_assets_of_nc  [10% ~ 60%]
    i = 0
    list = []
    list.append([df.stock_name[0], '经营活动产生的现金流量净额',\
            '购建固定资产、无形资产和其他长期资产支付的现金',\
            '处置固定资产、无形资产和其他长期资产收回的现金净额', \
            '购建固定资产、无形资产和其他长期资产支付的现金占比', \
            '处置固定资产、无形资产和其他长期资产收回的现金净额占比', 'result'])
    '''
    list.append([df.stock_name[0], 'ncf_from_oa', 'cash_paid_for_assets',\
        'net_cash_of_disposal_assets', 'paid_assets_of_ncf','disposal_assets_of_ncf', 'result'])
    '''
    for i in range(df_len):
        paid_assets_of_ncf      = df.cash_paid_for_assets[i] * 100   / df.ncf_from_oa[i]
        disposal_assets_of_ncf  = df.net_cash_of_disposal_assets[i] * 100 / df.ncf_from_oa[i]
        condi = paid_assets_of_ncf  > 10 and paid_assets_of_ncf < 60 
        list.append([df.record_date[i], \
            df.ncf_from_oa[i]/ y_unit, \
            df.cash_paid_for_assets[i] / y_unit, \
            df.net_cash_of_disposal_assets[i] / y_unit, \
            paid_assets_of_ncf , \
            disposal_assets_of_ncf, \
            condi])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret

#3-3
#bonus

#3-4
def income_analysis_ncf_of_oa_ia_fa(df):
    y_unit=10000*10000
    df_len=len(df)
    # ncf_from_oa > 0
    i = 0
    list = []
    list.append([df.stock_name[0], '经营活动产生的现金流量净额', \
        '投资活动产生的现金流量净额', '筹资活动现金流量净额', \
        'result'])
    #list.append([df.stock_name[0], 'ncf_from_oa', 'ncf_from_ia', 'ncf_from_fa', \
    #    'result'])
    for i in range(df_len):
        condi = df.ncf_from_oa[i] > 0 
        list.append([df.record_date[i], \
            df.ncf_from_oa[i]/ y_unit, \
            df.ncf_from_ia[i]/ y_unit, \
            df.ncf_from_fa[i]/ y_unit, \
            condi])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret


#3-5
def income_analysis_net_increase(df):
    y_unit=10000*10000
    df_len=len(df)
    # net_increase_in_cce > 0
    i = 0
    list = []
    list.append([df.stock_name[0], '现金及现金等价物净增加额', '期末现金及现金等价物余额', \
        'result'])
    #list.append([df.stock_name[0], 'net_increase_in_cce', 'final_balance_of_cce', \
    #    'result'])
    for i in range(df_len):
        condi = df.net_increase_in_cce[i] > 0 
        list.append([df.record_date[i], \
            df.net_increase_in_cce[i]/ y_unit, \
            df.final_balance_of_cce[i]/ y_unit, \
            condi])
    df_ret = pd.DataFrame(list)
    df_ret= df_ret.T
    return df_ret



def fina_data_analysis(df):
    all_df = df
    group_by_stock_code_df=all_df.groupby('stock_code')
    for stock_code, group_df in group_by_stock_code_df:
        ret_df = pd.DataFrame()
        '''
        if stock_code != 'SZ002475':
            continue
        '''
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

        ret_df = asset_df = income_analysis_assets(group_df)
        
        liab_df  = income_analysis_liab(group_df)
        ret_df = pd.concat([ret_df, liab_df]) 

        loan_df  = income_analysis_loan(group_df)
        ret_df = pd.concat([ret_df, loan_df]) 

        pay_recv_df = income_analysis_payable_receivable(group_df)
        ret_df = pd.concat([ret_df, pay_recv_df]) 

        fix_assets_df = income_analysis_fixed_assets(group_df)
        ret_df = pd.concat([ret_df, fix_assets_df]) 

        invest_df = income_analysis_invest(group_df)
        ret_df = pd.concat([ret_df, invest_df]) 

        roe_df = income_analysis_roe(group_df)
        ret_df = pd.concat([ret_df, roe_df]) 

        revenue_df = income_analysis_revenue(group_df)
        ret_df = pd.concat([ret_df, revenue_df]) 

        gross_df = income_analysis_gross(group_df)
        ret_df = pd.concat([ret_df, gross_df]) 

        costfee_df = income_analysis_costfee(group_df)
        ret_df = pd.concat([ret_df, costfee_df]) 

        main_profit_df = income_analysis_main_profit(group_df)
        ret_df = pd.concat([ret_df, main_profit_df]) 

        net_profit_df = income_analysis_net_profit(group_df)
        ret_df = pd.concat([ret_df, net_profit_df]) 

        paid_assets_df = income_analysis_paid_assets(group_df)
        ret_df = pd.concat([ret_df, paid_assets_df]) 

        ncf_df = income_analysis_ncf_of_oa_ia_fa(group_df)
        ret_df = pd.concat([ret_df, ncf_df]) 

        net_increase_df = income_analysis_net_increase(group_df)
        ret_df = pd.concat([ret_df, net_increase_df]) 

        ret_df.to_csv('./csv_data/' + stock_code + '_' + stock_name + '.csv', encoding='gbk')
    pass

def get_data_from_fina_income_balance_cashflow():

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


    '''
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
    '''

    return df



if __name__ == '__main__':

    df =  get_data_from_fina_income_balance_cashflow()
    group_df, target_df = fina_data_analysis(df)



