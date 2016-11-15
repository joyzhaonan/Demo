# # -*- coding: utf-8 -*-
#############################################
# Date : 2016-10-20   
# Author : ZhaoNan   
##实现功能:计算所有用户的持仓天数
import pandas as pd
from mysql.connector import conversion
import mysql
import time
import datetime
import numpy as np
from datebase_conntection import db_connection
from NpMySQLConverter import NumpyMySQLConverter

#输入：null
#功能：从数据库中获取用户合约相关信息，并存入user_contract_dataframe中
#      user_contract_dataframe的列包括：user_id,contract_id,transaction_start_time,transaction_end_time
#输出：user_contract_dataframe（用户合约dataframe）
def user_contract_df():
    user_contract_sql ='SELECT usr.userid AS user_id,' \
                 'crt.ContractId AS contract_id,' \
                 'DATE(crt.firstTradeDate) AS transaction_start_time,' \
                 'CASE  WHEN crt.realEndDate IS NULL THEN DATE(crt.endTradeDate)  ELSE  DATE(crt.realEndDate) END AS transaction_end_time ' \
                 'FROM 9niu_bi.t_dim_user usr ' \
                 'INNER JOIN 9niu_bi.t_dim_contract crt ' \
                 'ON usr.userid = crt.AccountId ' \
                 'ORDER BY userid,contractid limit 10'
    conn = mysql.connector.connect(**db_connection.config_9niu_bi)
    conn.set_converter_class(NumpyMySQLConverter)
    cursor = conn.cursor()
    global user_contract_dataframe
    user_contract_dataframe = pd.read_sql(user_contract_sql, conn)
    cursor.close()
    conn.close()
    return user_contract_dataframe

#输入:user_contract_dataframe（用户合约dataframe）
#功能:计算用户持仓天数
#输出:user_hold_position_days_dataframe（用户持仓天数dataframe）
def calculate_user_hold_position_days(user_contract_dataframe):
    global result
    result = []
    #userList = user_contract_df['用户ID'].drop_duplicates()
    #x=user_contract_df.columns
    user_list=user_contract_dataframe['user_id'].drop_duplicates()#user_id去重
    map(unique_user_days,user_list)##计算user_list中所有用户的持仓时间
    user_hold_position_days_dataframe = pd.DataFrame(result)
    #df.to_csv('F:\9niu\out.csv')
    return user_hold_position_days_dataframe

#输入：use_id
#功能：计算某个用户的持仓天数
#输出：把字典{user_id:**,hold_position_days:**}插入result列表中
def unique_user_days(user_id):
    dict = {}
    unique_user_contract_dataframe = user_contract_dataframe[(user_contract_dataframe['user_id'] == user_id)]
    rnglist = []
    # if str(uniq_user_contract_dataframe['contract_id'].values[0]) == 'nan':#没有合同的用户，设其持仓天数为0
    #     dict['user_id'] = user_id
    #     dict['hold_position_days'] = 0
    # else:
    def filldaytime(uniq_user_contract_df):##用的apply中的函数，把每一个合同的开始时间与结束时间中间的时间序列填满
        rng=pd.date_range(uniq_user_contract_df['transaction_start_time'],uniq_user_contract_df['transaction_end_time'])#填满时间序列
        [rnglist.append(item) for item in rng]#把一个区间的时间序列塞到rnglist中，列表生成式
    unique_user_contract_dataframe.apply(filldaytime,axis=1)##把时间序列填满和塞到rnglist中的动作分别应用到unique_user_contract_dataframe中的每一行
    rnglist = set(rnglist)##时间序列去重
    dict['user_id'] = user_id
    dict['hold_position_days'] = len(rnglist)##rnglist的长度即为该用户的持仓天数
    result.append(dict)##把该用户的持仓天数信息存进result中

#输入：user_hold_position_days_dataframe（用户持仓天数dataframe）
#功能：把user_hold_position_days_dataframe写入到到9niu_stage.t_hold_position_time values表中
#输出：落地数据库
def To_sql(user_hold_position_days_dataframe):
    conn = mysql.connector.connect(**db_connection.config_9niu_stage)
    conn.set_converter_class(NumpyMySQLConverter)
    cursor = conn.cursor()
    cursor.execute('truncate table t_hold_position_time')
    subdfs=np.array_split(df,10 )#防止内存不足,把df切成10份分布落地
    def split_insert(subdf):##把一份df，insert到数据库
        dflist = []
        [dflist.append([row['user_id'],row['hold_position_days']]) for index,row in subdf.iterrows()]
        cursor.executemany('insert into t_hold_position_time values(%s,%s)', dflist)
        conn.commit()
    map(split_insert,subdfs)##map实现多分df insert
    cursor.close()
    conn.close()

if __name__=="__main__":
    user_contract_df = user_contract_df()
    df = calculate_user_hold_position_days(user_contract_df)
    To_sql(df)




