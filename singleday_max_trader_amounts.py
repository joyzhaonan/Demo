# # -*- coding: utf-8 -*-
#############################################
# Date : 2016-10-28   
# Author : ZhaoNan   
##实现功能:计算所有用户的单日最大操盘金额
import pandas as pd
from mysql.connector import conversion
import mysql
import time
import datetime
import numpy as np
from datebase_conntection import db_connection
from NpMySQLConverter import NumpyMySQLConverter
#输入：null
#功能：从数据库中获取用户合约操盘金额信息，并存入user_contract_trade_amount_dataframe中
#      user_contract_trade_amount_dataframe的列包括：contract_id,user_id,trade_amountstransaction_start_time,transaction_end_time
#输出：user_contract_trade_amount_dataframe（用户合约操盘金额dataframe）
def user_contract_trade_amounts_df():
    user_contract_trade_amounts_sql ='SELECT id AS contract_id,' \
                                    'accountId AS user_id,' \
                                    'wfPercent AS trade_amounts,' \
                                    'DATE(firstTradeDate) AS transaction_start_time,' \
                 'CASE  WHEN DATE(realEndDate) IS NULL THEN DATE(endTradeDate) ELSE  DATE(realEndDate) END AS transaction_end_time ' \
                 'FROM 9niu_ods.`wftransation` WHERE accountId="43132330481801"'

    conn = mysql.connector.connect(**db_connection.config_9niu_bi)
    conn.set_converter_class(NumpyMySQLConverter)
    cursor = conn.cursor()
    global  user_contract_trade_amount_dataframe
    user_contract_trade_amount_dataframe = pd.read_sql(user_contract_trade_amounts_sql, conn)
    cursor.close()
    conn.close()
    return user_contract_trade_amount_dataframe

#输入:user_contract_trade_amount_dataframe（用户合约dataframe）
#功能:计算所有用户的单日最大操盘金额
#输出:user_sigleday_max_trader_amounts_dataframe（用户单日最大dataframe）
def calculate_user_max_trader_amounts(user_contract_trade_amount_dataframe):
    global result
    result=[]
    global  date_trade_amount
    date_trade_amount = []
    user_list=user_contract_trade_amount_dataframe['user_id'].drop_duplicates()#user_id去重
    map(unique_user_sigleday_max_trader_amounts,user_list)##计算user_list中所有用户
    global  user_sigleday_max_trader_amounts_dataframe
    user_sigleday_max_trader_amounts_dataframe = pd.DataFrame(result)
    user_sigleday_max_trader_amounts_dataframe.to_csv('F:\9niu\out.csv')
    return user_sigleday_max_trader_amounts_dataframe

#输入：use_id
#功能：计算某个用户单日最大操盘金额
#输出：把字典{user_id:**,:**}插入result列表中
def unique_user_sigleday_max_trader_amounts(user_id):
    dict = {}
    unique_user_contract_trader_amounts_dataframe = user_contract_trade_amount_dataframe[(user_contract_trade_amount_dataframe['user_id'] == user_id)]
    rnglist = []
    def fill_daytime_trader_amount(one_contract_trader_amounts):##用的apply中的函数，把每一个合同的开始时间与结束时间中间的时间序列填满
        ##开始交易时间>结束交易时间
        if one_contract_trader_amounts['transaction_start_time']>one_contract_trader_amounts['transaction_end_time']:
            dict2={}
            dict2['day_time']=one_contract_trader_amounts['transaction_end_time']##endtime
            dict2['day_trade_amounts']=one_contract_trader_amounts['trade_amounts']
            date_trade_amount.append(dict2)
        else:
        ##开始交易时间<结束交易时间,把时间序列填满，每一个时间序列对应一个操盘金额
            rng = pd.date_range(one_contract_trader_amounts['transaction_start_time'], one_contract_trader_amounts['transaction_end_time'])
            for one_day in rng:
                dict1={}
                dict1['day_time']=one_day
                dict1['trade_amount']=one_contract_trader_amounts['trade_amounts']
                date_trade_amount.append(dict1)
    #把fill_daytime_trader_amount应用到dataframe的每一行
    unique_user_contract_trader_amounts_dataframe.apply(fill_daytime_trader_amount,axis=1)
    #把填满的时间序列，trade_amount转换成df
    user_contract_daytime_df=pd.DataFrame(date_trade_amount)
    trader_amounts=user_contract_daytime_df['trade_amount'].groupby(user_contract_daytime_df['day_time']).sum().max()
    print trader_amounts
    dict['user_id']=user_id
    dict['trade_amount']=trader_amounts
    result.append(dict)

#输入：user_sigleday_max_trader_amounts_dataframe（用d单日最大操盘金额dataframe）
#功能：把user_sigleday_max_trader_amounts_dataframe写入到到9niu_stage.t_sigday_max_wfpercent表中
#输出：落地数据库
def To_sql(user_sigleday_max_trader_amounts_dataframe):
    conn = mysql.connector.connect(**db_connection.config_9niu_stage)
    conn.set_converter_class(NumpyMySQLConverter)
    cursor = conn.cursor()
    cursor.execute('truncate table t_hold_position_time')
    subdfs=np.array_split(user_sigleday_max_trader_amounts_dataframe,10 )#防止内存不足,把df切成10份分布落地
    def split_insert(subdf):##把一份df，insert到数据库
        dflist = []
        [dflist.append([row['user_id'],row['trade_amount']]) for index,row in subdf.iterrows()]
        cursor.executemany('insert into t_sigday_max_wfpercent values(%s,%s)', dflist)
        conn.commit()
    map(split_insert,subdfs)##map实现多分df insert
    cursor.close()
    conn.close()

if __name__=="__main__":
    user_contract_trade_amount_dataframe = user_contract_trade_amounts_df()
    df = calculate_user_max_trader_amounts(user_contract_trade_amount_dataframe)
    To_sql(df)
