# # -*- coding: utf-8 -*-
#############################################
# Date : 2016-11-04 
# Author : ZhaoNan  
#功能：计算；累计大盘增益，累计大盘增益率
import json
import pandas as pd
import time


class StockDataService(object):
    def __init__(self):
       pass

    @staticmethod
    def stockDataServiceDispatch(requestData):
        if requestData['type'] == 'indexProfit':
            backInfo = StockDataService.returnIndexProfit(requestData)
        return backInfo

    #输入：日期，天数
    #功能：计算大盘增益，大盘增益率
    #输出：{'indexProfitList'：[{date：*},{累计大盘收益：*}，{累计大盘收益率：*}]，'id':*}
    @staticmethod
    def returnIndexProfit():
        path = 'C:\Users\dell\Desktop\hs300.csv'
        #path = 'var/www/quant/pyalgotrade_cn/histdata/day/hs300.csv'
        indexDF = pd.read_csv(path, index_col=0)#第0列（Date Time）做行索引
        indexDF.sort_index(inplace=True,ascending=False)#按时间降序排列
       # print indexDF.to_string
        startDate = '2016-07-11'
        days = 5
        filteredIndexDF = indexDF[(indexDF.index.values <= startDate)]#只用2016-07-11以及之前的数据
        #days = '10'
        targetIndexDF = filteredIndexDF.iloc[0:int(days)+1]##取目标数据：2016-7-11日及前10天数据，共11天，可算10天的累积大盘收益
        targetIndexDF['earliest_Close']=targetIndexDF['Close'][-2]
        targetIndexDF['earliest_Close'][int(days)-1]=targetIndexDF['Close'][-1]
        targetIndexDF['accumulative_profit']=targetIndexDF['Close']-targetIndexDF['earliest_Close']
        targetIndexDF['accumulative_profit_percentage']=(targetIndexDF['Close']-targetIndexDF['earliest_Close'])/targetIndexDF['earliest_Close']*100
        targetDF=targetIndexDF[:int(days)]
        indexProfitList = []
        backInfo = {}
        insId = int(round(time.time() * 1000))
        for index, row in targetDF.iterrows():##目标数据每一行迭代地存进
            dailyIndexProfit = {}
            dailyIndexProfit.setdefault('date', index)#字典增加键-值对，时间
            dailyIndexProfit.setdefault('profit',str(round(row[6],2)))#字典增加键值对，大盘收益
            dailyIndexProfit.setdefault('profitPercentage', str(round(row[7],2)))#字典增加键值对，大盘收益率
            indexProfitList.append(dailyIndexProfit)
        backInfo.setdefault('id',insId)#backInfo增加键值对，id-当前时间
        backInfo.setdefault('indexProfitList',indexProfitList)#backInfo增加键值对，indexProfitList-收益list
        return backInfo

if __name__=="__main__":
    # obtainCurrentXRStocks()
    # writeDataToCsv('2015-09-01', '2016-09-08')
    # df = ts.get_h_data('601163',start='2015-09-01',end='2016-08-08')
    # print df
    StockDataService.returnIndexProfit()
    # refresh_index()



