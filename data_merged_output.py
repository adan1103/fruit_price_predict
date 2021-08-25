#!/usr/bin/env python
# coding: utf-8
import pandas as pd
import numpy as np
import pymysql
from sqlalchemy import create_engine
import re
import os
from functools import reduce
pd.options.mode.chained_assignment = None  # default='warn'

username = 'username'
password = 'password'
host_port = 'host_ip:host_port'
database = 'database_name'

engine = create_engine("mysql+pymysql://{}:{}@{}/{}".format(username, password, host_port, database))
con = engine.connect()

def df_cleaner(df):
    # step 1 讀取資料(選取需求檔案)
    cols = ['日期', '市場', '平均價(元/公斤)', '交易量(公斤)']
    df = df[cols]

    # step 2 清洗與補缺值
    ## 時間 -> 日期改成西元年(設成index)
    df["日期"] = df["日期"].apply(lambda x: re.sub("\d{3}", "{}".format((int(x.split("/")[0]) + 1911)), x))
    df["日期"] = pd.to_datetime(df["日期"])
    # df = df.set_index("日期")

    ## 去除成交量中的雜質(數據型態為object都處理)
    target_cols = list(df.select_dtypes("object"))
    df[target_cols] = df[target_cols].apply(lambda x: x.str.strip(" "))

    # 更改市場名稱
    df["市場"] = df["市場"].apply(lambda x:x.split(" ")[1])

    ## 找出缺值(re.sub) & 轉換型態
    try:
        df["平均價(元/公斤)"] = df["平均價(元/公斤)"].apply(lambda x:re.sub(",|-", "", x)).replace("", np.nan).fillna(method="ffill").astype("float")
        df["交易量(公斤)"] = df["交易量(公斤)"].apply(lambda x:re.sub(",|-", "", x)).replace("", np.nan).fillna(method="ffill").astype("float")
    except:
        pass

    return df


def df_merger(df, df_same, df_sub, fruit, market):
    
    product, same_type, substitution = fruit[0], fruit[1], fruit[2]
    
    df_market =  df.loc[df["市場"] == market]
    same = df_same.loc[df_same["市場"] == market]
    sub = df_sub.loc[df_sub["市場"] == market]

    dfs = [df_market, same, sub]
    df_merged = reduce(lambda left,right: pd.merge(left, right, on="日期", how="left"), dfs)

    #print(f"{market} 原始資料筆數(合併後): {df_merged.shape[0]}")
    
    # 將日期設為index
    df_merged.set_index("日期", inplace=True)

    # 篩選出必要欄位&重新命名
    cols = list(df_merged.columns) 
    df_merged = df_merged[cols[1:3] + [cols[4]] + [cols[7]]]
    df_merged.columns = [f"{product}_平均價", f"{product}_交易量", f"{same_type}_平均價", f"{substitution}_平均價"]

    # resample補齊每日資料(補值: 插值 -> 前值 -> 後值)
    df_merged = df_merged.resample("D").interpolate().fillna(method="ffill").fillna(method="bfill").applymap(lambda x: round(x,1))

    # 新增欄位(前日價格 & 5日移動平均)
    df_merged[f"{product}_前日平均價"] = df_merged[f"{product}_平均價"].shift(1).fillna(method="bfill")
    df_merged[f"{product}_5日平均價"] = round(df_merged[f"{product}_平均價"].rolling(5).mean().fillna(method="bfill"), 1)
    
    # 將日期還原為column
    df_merged.reset_index(inplace=True)
    
    return df_merged


fruit_list = ['banana', 'guava']
fruits = {"banana": ["banana", "scarletbanana", "guava"],
          "guava":  ["guava", 'emperorguava', 'banana']}
markets = ['台北二', '台北一', '三重區', '台中市']

for fruit_tmp in fruit_list:
    df = df_cleaner(pd.read_sql(f'marketing_price_{fruits[fruit_tmp][0]}', engine))
    df_same = df_cleaner(pd.read_sql(f'marketing_price_{fruits[fruit_tmp][1]}', engine))
    df_sub = df_cleaner(pd.read_sql(f'marketing_price_{fruits[fruit_tmp][2]}', engine))
    
    for markets_tmp in markets:
        df_final = df_merger(df, df_same, df_sub, fruits[fruit_tmp], market=markets_tmp)
#         print(f'{fruit_tmp}_{markets_tmp}')
#         print(df_final.dtypes)
#         print(df_final)
        df_org = pd.read_sql(f'{fruit_tmp}_{markets_tmp}', engine)
        duplicate = pd.merge(df_org, df_final, how='inner')
#         print(duplicate)
        for i in duplicate.index:
            df_final = df_final.drop(df_final.loc[(df_final['日期']==duplicate['日期'][i])].index[0])
#         print('update')
#         print(df_final)
        if df_final.index.size == 0:
            print(f'{fruit_tmp}_{markets_tmp} is already up to date ')
        else:
            print(f'{fruit_tmp}_{markets_tmp} is updating')
            df_final.to_sql(name=f'{fruit_tmp}_{markets_tmp}', con=con, if_exists='append', index=False)
#         
