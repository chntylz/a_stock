#!/usr/bin/env python
# -*- coding: utf-8 -*- 
import os,sys,time, datetime
import cgi

nowdate=datetime.datetime.now().date()
str_date= nowdate.strftime("%Y-%m-%d")

def cgi_generate_html_1(df):
    '''
    print """Content-type: text/html\r\n\r\n


    <html lang="zh">
      <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <meta http-equiv="refresh" content="5">
        <title>comm_update</title>
      </head>
      <body>
       %s 
      </body>
    </html>
    """ % (df.to_html())
    '''






    print("Content-type: text/html")
    print("")


    print("<html lang='zh'> ")
    print("  <head>")
    print("    <meta http-equiv='Content-Type' content='text/html; charset=UTF-8'>")
    print("    <meta http-equiv='refresh' content='5'>")
    print("    <title>comm_update</title>")
    print("  </head>")
    print("  <body>")

    print("  <h2> I am cgi </h2>")
    print("  %s " % df.to_html())
    print("  </body>")
    print("</html>")
    print("")

   
def cgi_handle_html_head(title_name, refresh=0):
    print("Content-type: text/html")
    print("")

    print('<!DOCTYPE html>\n')
    print('<html>\n')
    print('<head>\n')
    print('<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />\n')
    if refresh:
        print('<meta http-equiv="refresh" content="5">\n')
    print('<title> %s-%s </title>\n' % (title_name, datetime.datetime.now().date()))
    print('\n')
    print('\n')
    print('<style type="text/css">a {text-decoration: none}\n')
    print('\n')
    print('\n')

    print('/* gridtable */\n')
    print('table {\n')
    print('    font-size:18px;\n')
    print('    color:#000;\n')
    print('    border-width: 1px;\n')
    print('    border-color: #333333;\n')
    print('    border-collapse: collapse;\n')
    print('}\n')

    print('table tr {\n')
    print('    border-width: 1px;\n')
    print('    padding: 8px;\n')
    print('    border-style: solid;\n')
    print('    border-color: #333333;\n')
    print('}\n')


    print('table th {\n')
    print('    border-width: 1px;\n')
    print('    padding: 8px;\n')
    print('    border-style: solid;\n')
    print('    border-color: #333333;\n')
    print('}\n')

    print('table td {\n')
    print('    border-width: 1px;\n')
    print('    padding: 8px;\n')
    print('    border-style: solid;\n')
    print('    border-color: #333333;\n')
    print('}\n')

    '''
    print('    table tr:nth-child(odd){\n')
    print('    background-color: #eeeeee;\n')
    print('    }\n')
    '''

    print('/* /gridtable */\n')

    print('\n')
    print('\n')
    print('</style>\n')

    print('</head>\n')
    print('\n')
    print('\n')

def cgi_write_headline_column(df):

    print('    <tr>\n')
    #headline
    col_len=len(list(df))
    for j in range(0, col_len): 
        print('        <th>\n')
        print('        <a> %s</a>\n'%(list(df)[j]))
        print('        </th>\n')

    print('    </tr>\n')


def cgi_handle_link(stock_code):

    tmp_stock_code=stock_code
    if tmp_stock_code[0:1] == '6':
        stock_code_new='SH'+tmp_stock_code
    else:
        stock_code_new='SZ'+tmp_stock_code
        
    xueqiu_url='https://xueqiu.com/S/' + stock_code_new
    hsgt_url='../../cgi-bin/hsgt-search.cgi?name=' + tmp_stock_code
    cgi_url = xueqiu_url + '/detail#/ZYCWZB'    
    return xueqiu_url, hsgt_url, cgi_url
    
    
def cgi_write_to_file( df):
    print('<table >\n')

    #headline
    cgi_write_headline_column(df)

    #dataline
    #print('%s\n'%(list(df)))
    df_len=len(df)
    for i in range(0, df_len): #loop line

        print('    <tr>\n')
        a_array=df[i:i+1].values  #get line of df
        tmp_stock_code=a_array[0][1] 
        xueqiu_url, hsgt_url, cgi_url = cgi_handle_link(tmp_stock_code)

        col_len=len(list(df))
        for j in range(0, col_len): #loop column
            print('        <td>\n')
            element_value = a_array[0][j] #get a[i][j] element
            #df_cgi_column=['record_date', 'stock_code', 'stock_name', 'or_yoy', 'netprofit_yoy', 'conti_day']
            if(j == 0): 
                print('           <a href="%s" target="_blank"> %s[fina]</a>\n'%(cgi_url, element_value))
            elif(j == 1): 
                print('           <a href="%s" target="_blank"> %s[hsgt]</a>\n'%(hsgt_url, element_value))
            elif(j == 2):
                print('           <a href="%s" target="_blank"> %s</a>\n'%(xueqiu_url, element_value))
            elif(j == col_len - 1):
                print('           <a> %.2f</a>\n'%(element_value))
            else:
                print('           <a> %s</a>\n'%(element_value))
                     
                                
            print('        </td>\n')
        print('    </tr>\n')

    print('</table>\n')

    pass

def cgi_hsgt_part_body():
    print ('   <form action="hsgt-search.cgi">')
    print ('   code or name <input type="text" name="name" />')
    print ('   <input type="submit" />')
    print ('   </form>')
    print ('   <a href="%s" target="_blank"> [picture]</a>' % ('../html/test.png'))
    print ('   <p></p>')
    pass
    
def cgi_handle_html_body(df, form=0):
    print('<body>\n')
    print('\n')
    print('\n')
    print('\n')
    #print('<p>----------------------------------------------------------------------</p>\n')
    #print('<p>----------------------------------------------------------------------</p>\n')
    print('\n')
    print('\n')

    if form:
        cgi_hsgt_part_body()

    cgi_write_to_file(df)

    print('        <td>\n')
    print('        </td>\n')
    print('</body>\n')
    pass

def cgi_handle_html_end():
    print('\n')
    print('\n')
    print('</html>\n')
    print('\n')

    pass
    
   


def comm_get_hsgt_continous_info(df):
    hsgt_df = df
    hsgt_df_len = len(hsgt_df)
    money_total = 0
    flag_m = hsgt_df.loc[0]['delta1_m']
    if flag_m > 0:
        conti_flag = 1
    else:
        conti_flag = 0

    for i in range(hsgt_df_len):
        delta_m = hsgt_df.loc[i]['delta1_m']

        if delta_m >= 0:
            tmp_flag = 1
        else:
            tmp_flag = 0

        if conti_flag == tmp_flag:
            money_total = money_total + delta_m
        else:
            break

    money_total = round(money_total, 2)

    return i, money_total
 

def comm_handle_hsgt_data(df):

    all_df =df 

    del all_df['open']
    del all_df['high']
    del all_df['low']
    del all_df['volume']

    if len(all_df) > 0:
        #the_first_line - the_second_line
        all_df['delta1']  = all_df.groupby('stock_code')['percent'].apply(lambda i:i.diff(-1))
        all_df['delta1_share'] = all_df.groupby('stock_code')['share_holding'].apply(lambda i:i.diff(-1))
        all_df['delta1_m'] = all_df['close'] * all_df['delta1_share'] / 10000;
        del all_df['delta1_share']

        #delete 
        #all_df=all_df[all_df['delta1_m'] != 0]
        #all_df=all_df.reset_index(drop=True)



    hsgt_df = all_df

    hsgt_df_len = len(hsgt_df)
    if hsgt_df_len > 1:
        hsgt_date           = hsgt_df['record_date'][0]
        hsgt_share          = hsgt_df['share_holding'][0]
        hsgt_percent        = hsgt_df['percent'][0]
        hsgt_delta1         = hsgt_df['percent'][0] - hsgt_df['percent'][1]
        hsgt_delta1         = round(hsgt_delta1, 2)
        hsgt_deltam         = (hsgt_df['share_holding'][0] - hsgt_df['share_holding'][1]) * hsgt_df['close'][0] /10000.0
        hsgt_deltam         = round(hsgt_deltam, 2)
        conti_day, money_total= comm_get_hsgt_continous_info(hsgt_df)
    elif hsgt_df_len > 0:
        hsgt_date           = hsgt_df['record_date'][0]
        hsgt_share          = hsgt_df['share_holding'][0]
        hsgt_percent        = hsgt_df['percent'][0]
        hsgt_delta1         = hsgt_df['percent'][0]
        hsgt_deltam         = hsgt_df['share_holding'][0] * hsgt_df['close'][0]/10000.0
        hsgt_deltam         = round(hsgt_deltam, 2)
        conti_day             = 1
        money_total         = hsgt_deltam
    else:
        hsgt_date           = ''
        hsgt_share          = 0
        hsgt_percent        = 0
        hsgt_delta1         = 0
        hsgt_deltam         = 0
        conti_day             = 0
        money_total         = 0


    return hsgt_date, hsgt_share, hsgt_percent, hsgt_delta1, hsgt_deltam, conti_day, money_total   
    
