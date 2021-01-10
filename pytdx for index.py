'''
失败原因：接口不稳定
'''



'''
首先随意获得一个以日为周期的指数历史数据
这样我们就能通过这份数据来获得过去交易日的日期
命名为date_df
'''

import pandas as pd

# from pytdx.exhq import TdxExHq_API
# from pytdx.reader import TdxLCMinBarReader, TdxFileNotFoundException
from pytdx.hq import TdxHq_API

api = TdxHq_API(auto_retry=True)
df = pd.DataFrame()

period_list = [3, 7, 15, 30, 60, 90, 150]


bk_df = pd.read_excel('通达信板块指数20210109.xlsx')[:-1] #-1 remove last row
code_list = bk_df['代码']
# code_list = ['880330']
print(code_list)

date_df = pd.read_excel('date.xls')[98:100]
date_list = date_df['时间'].tolist()
date_list_reverse = list(reversed(date_list))

def get_data(code_str):
    try:
        if api.connect('119.147.212.81', 7709):
            name = bk_df[bk_df['代码'] == code]['名称'].iloc[0]
            print(code_str)
            yest_data_dic = api.get_security_bars(9, 1, code_str, index+1, 1)
            yest_close = yest_data_dic[0]['close']
            now_data_dic = api.get_security_bars(9, 1, code_str, index, 1)  # data of now
            now_close = now_data_dic[0]['close']
            now_year = str(now_data_dic[0]['year'])
            now_month = str(now_data_dic[0]['month'])
            if now_data_dic[0]['month'] < 10:
                now_month = '0' + now_month
            now_day = str(now_data_dic[0]['day'])
            if now_data_dic[0]['day'] < 10:
                now_day = '0' + now_day
            now_date = now_year + now_month + now_day
            now_change = (now_close - yest_close) / yest_close
            temp_list = [
                now_date, code, name,
                now_change,
                ]
            for period in period_list:
                # 日线、上交、code、截止时间、最后一根
                data_dic = api.get_security_bars(9,1, code_str, period, 1)
                if len(data_dic) !=0:
                    close = data_dic[0]['close']
                    change = (now_close - close) / close
                else:
                    change = ''
                temp_list.append(change)
            data_list.append(temp_list)
            # return data_list
            # api.disconnect()
        else:
            print('connection failed')
    except TimeoutError:
        print('failed')
        get_data(code_str)


for date in date_list_reverse:
    data_list = []
    print(date)
    index = date_list_reverse.index(date)
    for code in code_list:
        code_str = str(code)

        get_data(code_str)


        column_name = [
                    '日期', '代码', '名称',
                    '涨幅1', '涨幅3', '涨幅7', '涨幅15', 
                    '涨幅30', '涨幅60', '涨幅90', '涨幅150'
                    ]
        df_temp = pd.DataFrame(data_list, columns=column_name)
        df = df_temp.append(df).reset_index(drop=True)


df.to_excel('板块强度20210109.xlsx')