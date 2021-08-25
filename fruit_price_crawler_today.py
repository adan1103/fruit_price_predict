#!/usr/bin/env python
# coding: utf-8

# define
from bs4 import BeautifulSoup
import requests
import csv
import pandas as pd
import re
import json
import os 
import time
import datetime
import numpy as np
from selenium import webdriver 
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import Chrome
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.action_chains import ActionChains
import pymysql
from sqlalchemy import create_engine
import jieba
import jieba.analyse
from pymongo import MongoClient
import mysql.connector

# Global variable
mongodb_atlas_account = "account"
mongodb_atlas_password = "password"

mysql_username = 'username'
mysql_password = 'password'
host_port = 'host_ip:host_pott'
database = 'database_name'

engine = create_engine("mysql+pymysql://{}:{}@{}/{}".format(mysql_username, mysql_password, host_port, database))
con = engine.connect()
ss = requests.session()


def date_to_str(tmp):
    if tmp >= 10:
        tmp_str = str(tmp)
    else:
        tmp_str = '0'+str(tmp)
    return tmp_str


def title_mining(tmp_t):
    return jieba.analyse.extract_tags(tmp_t, topK=4, withWeight=True, allowPOS=('n', 'nr', 'ns', 'nz', 'v', 'vd', 'vn'))


def content_mining(tmp_c):
    return jieba.analyse.extract_tags(tmp_c, topK=20, withWeight=True, allowPOS=('n', 'nr', 'ns', 'nz', 'v', 'vd', 'vn'))


def news_jieba(function_name, ID_jieba, title_jieba, content_jieba):
    content_list = []
    title_list = []
    
    # 因為有不同文章來源，為了區別使用爬蟲網站縮寫+_id, coa=農委會, afa=農糧署
    title_col = [function_name+'_id','key_1', 'value_1', 'key_2', 'value_2', 'key_3', 'value_3', 'key_4', 'value_4']
    content_col = [function_name+'_id','key_1', 'value_1', 'key_2', 'value_2', 'key_3', 'value_3', 'key_4', 'value_4', 'key_5', 'value_5',
            'key_6', 'value_6', 'key_7', 'value_7', 'key_8', 'value_8', 'key_9', 'value_9', 'key_10', 'value_10',
            'key_11', 'value_11', 'key_12', 'value_12', 'key_13', 'value_13', 'key_14', 'value_14', 'key_15', 'value_15',
            'key_16', 'value_16', 'key_17', 'value_17', 'key_18', 'value_18', 'key_19', 'value_19', 'key_20', 'value_20']

    for i in range(len(ID_jieba)):
        # title與content分別進入Text mining function處理
        tmp_title = title_mining(title_jieba[i])
        tmp_content = content_mining(content_jieba[i])
        
        # 清空list
        title_keyword = []
        content_keyword = []
        
        # 第一欄加入文章ID以辨識此欄位是哪篇文章
        title_keyword.append(int(ID_jieba[i]))
        content_keyword.append(int(ID_jieba[i]))
        
        # jieba分詞完是一個tuple包含分詞與詞頻的狀態，為了方便存取，將兩者拆開
        for i in range(4):
            # 標題
            if i >= len(tmp_title):
                # 若標題找到的關鍵字小於4個(topK=4)則key填入"NA",value填0,若這邊是空值會導致dataframe columns長度不符
                title_keyword.append('NA')
                title_keyword.append(0)    
            else:
                title_keyword.append(tmp_title[i][0]) # 分詞 str
                title_keyword.append(tmp_title[i][1]) # 詞頻 float
                
        # 若內文找到的關鍵字小於20個(topK=20)則則key填入"NA",value填0,若這邊是空值會導致dataframe columns長度不符
        for i in range(20):
            # 內容
            if i >= len(tmp_content):
                title_keyword.append('NA')
                title_keyword.append(0)
            else:
                content_keyword.append(tmp_content[i][0]) # 分詞 str
                content_keyword.append(tmp_content[i][1]) # 詞頻 float
        #此篇文章處理完就加入list中 -> [[文章1],[文章2]...]
        title_list += [title_keyword]   
        content_list += [content_keyword]
        
    # 建立DataFrame並return
    df_title = pd.DataFrame(np.array(title_list), columns=title_col)
    df_content = pd.DataFrame(np.array(content_list), columns=content_col)
   
    return(df_title, df_content)


# In[7]:


def afa_news(page_tmp):
    
    print('afa_news crawler start')  
    def get_info(sub_soup):
        # 進入function後每一個try都會執行，若發生錯誤或取不到值則讓他顯示 Error方便debug
        # 對照圖(二)單則農業新聞
        try:               
            title_t = sub_soup.select('div[class="col-sm-9"]')[0].text
        except:
            title_t = 'title Error'
        try:                
            content_t = sub_soup.select('article[class="shared-content-text"]')[0].text
        except:
            content_t = 'content Error'
        try:              # text會取到"發布日期：110-06-02"，後面再用split切割成 ["發布日期：", "110-06-02"]，取第二個
            post_date = sub_soup.select('div[class="agricultural-news-content-title row mb-lg"]')[0].text.split('發布日期：')[1]
        except:
            post_date = 'post_date Error'

        # return只會傳str，需要將上面取得之內容放進list內整個回傳， 否則會只回傳第一個字元     
        tmp_list = [re.sub('[-:_、【】。；：)(「」，.&+\n\t\r\u3000]', ' ',title_t),
                    re.sub('[-:_、【】。；：)(「」，.&+\n\t\r\u3000]', ' ', content_t),
                    re.sub('-','/',post_date)]
        return tmp_list[0], tmp_list[1], tmp_list[2]

    def get_sub_link(urls):
        sub_link=[]
        res = requests.session().get(url=urls, headers=headers)
        soup = BeautifulSoup(res.text, 'html.parser')
        for i in range(0, len(soup.select('a[class="article_class"]'))):
            sub_link.append(soup.select('a[class="article_class"]')[i]['href'])
        return sub_link

    url = []
    link = []
    out = []
    title = []
    content = []
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
           }
    org_url = 'https://www.afa.gov.tw/cht/index.php?code=list&ids=307'
#     page_tmp = 1
    for i in range(1, page_tmp+1):
        url.append(org_url+'&page={}'.format(i))

    # 從主頁面get request並用BeautifulSoup轉換成html，用開發工具發現子頁面的連結在a標籤的'article_class'屬性中
    # 存取該標籤的'href'，並存到link中

    # 同時建立及啟用10個執行緒
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(get_sub_link, url)

    link_tmp = [i for i in results]  
    #將link中的article_id當成存進資料庫後的唯一識別
    link = []

    for i in range(len(link_tmp)):
        for j in link_tmp[i]:
            link.append(j)
    ID = list(map(lambda x: x.split('&article_id=')[1], link))  

    # 透過個連結逐一訪問子頁面
    for j in range(len(link)):
        # print('進入子新聞頁面', j+1)
        res_sub = ss.get(url=link[j], headers=headers)
        sub_soup_main = BeautifulSoup(res_sub.text, 'html.parser')

        # 將BeautifulSoup處理過的html代入函式處理，主要程式流程看起來比較乾淨
        # html帶入get_info執行完會回傳3個str，分別是標題、內容、發布日期，因此我們要用3個變數暫存再放進list中
        title_tmp, content_tmp, tmp_date = get_info(sub_soup_main)
        # 將多個空白改成一個空白 
        title.append((' '.join(title_tmp.split())))
        content.append((' '.join(content_tmp.split())))
        out.append([int(ID[j]), tmp_date, (' '.join(title_tmp.split())), (' '.join(content_tmp.split())), link[j]])
    print('afa_news crawler finish')  

    print('afa_news Text mining start')
    df_title, df_content = news_jieba("afa", ID, title, content)
    print('afa_news Text mining finish')

    # 建立連線並定義collections名稱
    client = MongoClient('mongodb+srv://{}:{}@twfruit.i2omj.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'.format(mongodb_atlas_account, mongodb_atlas_password))
    db = client.TWFruits
    
    afa_news_title_jieba = db.afa_news_title_jieba
    afa_news_content_jieba = db.afa_news_content_jieba
    afa_news = db.afa_news

    # 定義數字型態的columns並將該欄位所有rows轉換成數字型態
    news_col = ['afa_id', 'date', 'title', 'content', 'link']
    title_cols = ['afa_id','value_1', 'value_2', 'value_3', 'value_4']
    content_cols = ['afa_id', 'value_1','value_2', 'value_3','value_4', 'value_5',
                   'value_6', 'value_7',  'value_8',  'value_9',  'value_10',
                  'value_11', 'value_12', 'value_13', 'value_14', 'value_15',
                  'value_16', 'value_17', 'value_18', 'value_19', 'value_20']
    df_afa_news = pd.DataFrame(np.array(out), columns=news_col)

    df_title[title_cols] = df_title[title_cols].apply(pd.to_numeric)
    df_content[content_cols] = df_content[content_cols].apply(pd.to_numeric)
    df_afa_news['afa_id'] = df_afa_news['afa_id'].apply(pd.to_numeric)

    
    print("afa_news update to mongodb -> start")
    # 判斷id是否存在於mongodb中，若無則寫進資料庫
    for excist_id in df_afa_news['afa_id']:
        # print(excist_id)
        if [x for x in afa_news.find({"afa_id":int(excist_id)})] == []:
            afa_news_update = df_afa_news.loc[df_afa_news["afa_id"]==excist_id].to_dict(orient='records')
            updated = afa_news.insert_one(afa_news_update[0]).inserted_id
            print("afa_news update id ", updated)

    for excist_id in df_title['afa_id']:
        if [x for x in afa_news_title_jieba.find({"afa_id":int(excist_id)})] == []:
            afa_news_title_jieba_update = df_title.loc[df_title["afa_id"]==excist_id].to_dict(orient='records')
            # print(afa_news_title_jieba_update[0])
            updated = afa_news_title_jieba.insert_one(afa_news_title_jieba_update[0]).inserted_id
            print("afa_news title jieba update id ", updated)

    for excist_id in df_content['afa_id']:
        if [x for x in afa_news_content_jieba.find({"afa_id":int(excist_id)})] == []:
            afa_news_content_jieba_update = df_content.loc[df_content["afa_id"]==excist_id].to_dict(orient='records')
            updated = afa_news_content_jieba.insert_one(afa_news_content_jieba_update[0]).inserted_id
            print("afa_news content jieba update id ", updated)

    print("afa_news update to mongodb -> finish")
    client.close()

    return

def coa_news(start_year_tmp, start_month_tmp, end_year_tmp, end_month_tmp):
    
    print('coa_news crawler start') 
    form_data = {
    'keyword': '',
    'division_lv1': '*',
    'year': start_year_tmp,
    'month': start_month_tmp,
    'end_year': end_year_tmp,
    'end_month': end_month_tmp,
    'search_Submit': '查詢',
    'is_search': 'y'
    }

    headers = {
        'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
    }
    url = 'https://www.coa.gov.tw/theme_list.php?theme=news&sub_theme=agri'
    ss = requests.session()
    res = ss.post(url=url, headers=headers, data=form_data)
    soup = BeautifulSoup(res.text, 'html.parser')
    out = []
    ID = []
    date = []
    title = []
    author = []
    link = []
    content = []

    # 取發布日期
    for b in range(1, len(soup.select('td[align="center"]')), 3):
        newsDate = soup.select('td[align="center"]')[b]
        date.append(newsDate.text.replace('-','/'))

    # 取發布機關
    for c in range(2, len(soup.select('td[align="center"]')), 3):
        newsAuthor = soup.select('td[align="center"]')[c]
        author.append(newsAuthor.text)

    # 取新聞標題、網址
    for i in range(0, len(soup.select('a[class="main-c9-index"]'))):
        newsTitle = soup.select('a[class="main-c9-index"]')[i]['title']
        newsLink = 'https://www.coa.gov.tw/' + soup.select('a[class="main-c9-index"]')[i]['href']
        title.append(re.sub('[-:_【】)(「」&+\n\t\r\u3000\xa0]', ' ', newsTitle))
        link.append(newsLink)

    # 取新聞內容、文號
    for j in range(len(link)):
        page_res = ss.get(url=link[j], headers=headers)
        page_soap = BeautifulSoup(page_res.text, 'html.parser')

        # 災情報告文號為"HOT"，改取連結後面的id
        ID_tmp = page_soap.select('td[class="word-2"]')[0].text.split("：")[1]
        if ID_tmp.isdigit():
            ID.append(ID_tmp)
        else:
            ID.append(link[j].split("&id=")[1])

        for w in page_soap.select('div[class="word"]'):
            content.append(re.sub('[-:_【】)(「」&+\n\t\r\u3000\xa0]', ' ', w.text))

    for k in range(len(ID)):
        out.append([ID[k], date[k], author[k], title[k], content[k], link[k]])
#     print('out=', out)
    print('coa_news crawler finish')  

    # 定義數字型態的columns並將該欄位所有rows轉換成數字型態
    news_col = ['coa_id', 'date', 'author', 'title', 'content', 'link']
    title_cols = ['coa_id','value_1', 'value_2', 'value_3', 'value_4']
    content_cols = ['coa_id', 'value_1','value_2', 'value_3','value_4', 'value_5',
                   'value_6', 'value_7',  'value_8',  'value_9',  'value_10',
                  'value_11', 'value_12', 'value_13', 'value_14', 'value_15',
                  'value_16', 'value_17', 'value_18', 'value_19', 'value_20']

    df_coa_news = pd.DataFrame(np.array(out), columns=news_col)
    df_coa_news['coa_id'] = df_coa_news['coa_id'].apply(pd.to_numeric)

    # text mining
    print('coa_news Text mining start')
    df_title, df_content = news_jieba("coa", ID, title, content)
    print('coa_news Text mining finish')

    # 建立連線並定義collections名稱
    client = MongoClient('mongodb+srv://{}:{}@twfruit.i2omj.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'.format(mongodb_atlas_account, mongodb_atlas_password))
    db = client.TWFruits
    
    coa_news_title_jieba = db.coa_news_title_jieba
    coa_news_content_jieba = db.coa_news_content_jieba
    coa_news = db.coa_news


    df_title[title_cols] = df_title[title_cols].apply(pd.to_numeric)
    df_content[content_cols] = df_content[content_cols].apply(pd.to_numeric)
    
    # Demo
#     print(df_coa_news)
#     print("--------------------")
#     print(df_title)
#     print("====================")
#     print(df_content)

    # 判斷id是否存在於mongodb中，若無則insert
    print("coa_news update to mongodb -> start")

    for excist_id in df_coa_news['coa_id']:
        if [x for x in coa_news.find({"coa_id":int(excist_id)})] == []:
            coa_news_update = df_coa_news.loc[df_coa_news["coa_id"]==excist_id].to_dict(orient='records')
            updated = coa_news.insert_one(coa_news_update[0]).inserted_id
            print("coa_news update id ", updated)

    for excist_id in df_title['coa_id']:
        if [x for x in coa_news_title_jieba.find({"coa_id":int(excist_id)})] == []:
            coa_news_title_jieba_update = df_title.loc[df_title["coa_id"]==excist_id].to_dict(orient='records')
    #         print(afa_news_title_jieba_update[0])
            updated = coa_news_title_jieba.insert_one(coa_news_title_jieba_update[0]).inserted_id
            print("coa_news title jieba update id ", updated)


    for excist_id in df_content['coa_id']:
        if [x for x in coa_news_content_jieba.find({"coa_id":int(excist_id)})] == []:
            coa_news_content_jieba_update = df_content.loc[df_content["coa_id"]==excist_id].to_dict(orient='records')
            updated = coa_news_content_jieba.insert_one(coa_news_content_jieba_update[0]).inserted_id
            print("coa_news content jieba update id ", updated)

    print("coa_news update to mongodb -> finish")
    client.close()

    return

def news_merge():
    print('news merge -> start')
    client = MongoClient('mongodb+srv://{}:{}@twfruit.i2omj.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'.format(mongodb_atlas_account, mongodb_atlas_password))
    db = client.TWFruits # connect database TWFruits

    # 分別讀取afa_news和coa_news並轉換成dataframe進行讀取
    # connect collection afa_news
    afa_news = db.afa_news
    df_afa = pd.DataFrame(list(afa_news.find()))

    # connect collection coa_news
    coa_news = db.coa_news
    df_coa = pd.DataFrame(list(coa_news.find()))

    df_coa = df_coa[['coa_id', 'date', 'title', 'content', 'link']]
    df_coa = df_coa.rename(columns={'coa_id': 'news_id'})

    df_afa = df_afa[['afa_id', 'date', 'title', 'content', 'link']]
    df_afa = df_afa.rename(columns={'afa_id': 'news_id'})

    df_news = pd.concat([df_afa, df_coa])
    df_news = df_news.drop_duplicates(subset=['title', 'date'])

    news = db.news

    for excist_id in df_news['news_id']:
        if [x for x in news.find({"news_id":int(excist_id)})] == []:
            news_update = df_news.loc[df_news["news_id"]==excist_id].to_dict(orient='records')
            print(news_update)
            updated = news.insert_one(news_update[0]).inserted_id
            print("news update id ", updated)

    # 全部更新
    # news_data_update = df_news.to_dict(orient='records')
    # news_data
    # out = news_data.insert_many(news_data_update)
    # print('news_data update id =', out)
    client.close()
    print('news merge -> finish')
    return

def marketing_price_soup(fruit_name, table_content_tmp):

    Header = table_content_tmp[0:10]
    Header[7] = '價格跟前一交易日比較%'
    Header[9] = '交易量跟前一交易日比較%'
    print('marketing_price crawler -> finish') 
    
    #清洗資料
    data = table_content_tmp[18:]
    output=[]
    for s_data in range(0, len(data), 10):
        output.append(data[s_data:s_data+10])    
    df3 = pd.DataFrame(output,columns=Header)
    # 按日期排序，reset_index後會多出index欄位，後面讀回的原始資料沒有index因此一併去除
    df3 = df3.sort_values(["日期"], ascending=True).reset_index().drop(['index'], axis=1)
    
    # 將交易量中的","去除並轉成數字
    df3["交易量(公斤)"] = df3["交易量(公斤)"].apply(lambda x: int(" ".join(re.sub(",", "", x).split())))
    
    # dataFrame部分欄位轉成數字型態
    col = ["上價", "中價", "下價", "平均價(元/公斤)", "交易量(公斤)"]
    df3[col] = df3[col].apply(pd.to_numeric)
    
    df3 = df3.drop(['交易量跟前一交易日比較%', '價格跟前一交易日比較%'], axis=1)
    df3['市場'] = df3['市場'].apply(lambda x: x.strip(" "))
#     df3['市場'] = df3['市場'].apply(lambda x: x.rstrip(" ").replace(" ", "-"))
#     df3['產品'] = df3['產品'].apply(lambda x: x.rstrip(" ").replace(" ", "-"))
#     df3.to_csv("farmproduct_{}_crawler.csv".format(fruit_name), index=False)
#     print(df3.dtypes)
#     print(df3)
#    print("marketing_price update to mongodb -> start")
    # 寫進mongodb
#     client = MongoClient('mongodb+srv://{}:{}@twfruit.i2omj.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'.format(mongodb_atlas_account, mongodb_atlas_password))
#     db = client.TWFruits
#     marketing_price_data = db.price_marketing
    
#     用日期與市場欄位判斷資料是否已存在，若無則寫入資料庫
#     for excist_time, excist_marketing, excist_fruit,  in zip(df3['日期'], df3['市場'], df3['產品']):
#         print(excist_time,excist_marketing)
#         if [x for x in marketing_price_data.find({"日期":excist_time, "市場":excist_marketing, "產品":excist_fruit})] == []:
#             marketing_price_data_update = df3.loc[(df3["日期"]==excist_time) & (df3["市場"]==excist_marketing) & (df3["產品"]==excist_fruit)].to_dict(orient='records')
#             updated = marketing_price_data.insert_one(marketing_price_data_update[0]).inserted_id
#             print("marketing_price_data update id ", updated)
#     print("marketing_price update to mongodb -> finish")
    
    print("marketing_price update to mysql -> start")
#     fruit_type = fruit_name
    engine = create_engine("mysql+pymysql://{}:{}@{}/{}".format(mysql_username, mysql_password, host_port, database))
    con = engine.connect()
    df_read = pd.read_sql('marketing_price_{}'.format(fruit_name), engine)
#     print(df_read.dtypes)
#     print(df_read)
    # 去除日期重複的欄位並寫入mysql
#     df_concat = pd.concat([df_read, df3], join='inner')
#     df_concat = df_concat.drop_duplicates(subset=['日期', '市場'], keep='first')
#     print('df_concat = ',df_concat)
    
    duplicate = pd.merge(df_read, df3, how='inner')
#     print('duplicate=', duplicate)
    for i in duplicate.index:
        df3 = df3.drop(df3.loc[(df3['日期']==duplicate['日期'][i]) & (df3['市場']==duplicate['市場'][i])].index[0])

    print('update = ',df3)
    df3.to_sql(name='marketing_price_{}'.format(fruit_name), con=con, if_exists='append', index=False)
    con.close
    
    print("marketing_price update to mysql -> finish")
    
    return


def marketing_price(fruit, start_date):

    print('marketing_price crawler -> start') 
    fruit_select = {"banana":"A1", "scarletbanana":"A2", "pineapple":"B2","guava":"P1","emperorguava":"P3"}
    url = "https://amis.afa.gov.tw/fruit/FruitProdDayTransInfo.aspx"

    start_time = time.time()

    chrome_options = Options()
    chrome_options.add_argument("--headless")

#    driver = webdriver.Chrome(options=chrome_options, executable_path='~/home/adan7575/crawler/chromedriver')
    driver = webdriver.Chrome(options=chrome_options, executable_path='./chromedriver')

    # driver = Chrome("./chromedriver")
    driver.get(url)

    #### 1. 選取範圍 => 期間
    driver.find_element_by_xpath("//*[@id='ctl00_contentPlaceHolder_ucDateScope_rblDateScope_1']").click()
    time.sleep(2)

    #### 2. 選取日期
    # 執行js語法來解除只能read的input格子
    driver.execute_script("$('input[id=ctl00_contentPlaceHolder_txtSTransDate]').removeAttr('readonly')")

    # 清空既有input並放入keys
    driver.find_element_by_id('ctl00_contentPlaceHolder_txtSTransDate').clear() 
    driver.find_element_by_id('ctl00_contentPlaceHolder_txtSTransDate').send_keys(start_date)
    # time.sleep(2)

    #### 3. 選取市場(目前code僅能選全部市場)
    driver.find_element_by_xpath("//*[@id='ctl00_contentPlaceHolder_txtMarket']").click() 
    #time.sleep(3)

    # 點選後轉移到跳出視窗，選取全部市場
    iframe = driver.find_elements_by_tag_name("iframe")[0]
    driver.switch_to.frame(iframe)
    radio_target = driver.find_element_by_xpath("//*[@id='radlMarketRange_0']")
    radio_target.click()

    #### 4. 選取水果種類 
    driver.find_element_by_xpath("//*[@id='ctl00_contentPlaceHolder_txtProduct']").click()
    #time.sleep(3)

    # 點選後轉移到跳出視窗
    iframe = driver.find_elements_by_tag_name("iframe")[0]
    driver.switch_to.frame(iframe)

    # 抓取下拉選單元件，直接以值來選擇
    select = Select(driver.find_element_by_name('lstProduct'))
    select.select_by_value(fruit_select[fruit])

    # 選取完成後，關閉視窗
    driver.find_element_by_xpath("//*[@id='btnConfirm']").click()

    #### 5. 點選查詢button
    driver.find_element_by_xpath("//*[@id='ctl00_contentPlaceHolder_btnQuery']").click()

    # 有時候網頁跑很久，sleep太少會拿到錯誤的page_source
    time.sleep(10)
    #     html = driver.execute_script("return document.getElementsByTagName('html')[0].outerHTML")

    soup = BeautifulSoup(driver.page_source,'html.parser')

    # time.sleep(60)

    table = soup.select("table[border='1']")
    table_content = list(filter(None,table[0].text.split('\n')))
    driver.close()
    marketing_price_soup(fruit, table_content)
    return 

def weather_predict():

    # 授權碼 
    authorization = 'CWB-60FC0788-DF06-4574-9E72-874260AC7B12'

    # 麟洛、燕巢、中寮、員林(按順序)
    location_name = ['%E9%BA%9F%E6%B4%9B%E9%84%89', '%E7%87%95%E5%B7%A2%E5%8D%80', '%E4%B8%AD%E5%AF%AE%E9%84%89', '%E5%93%A1%E6%9E%97%E5%B8%82']
    codename = ['F-D0047-035', 'F-D0047-067', 'F-D0047-023', 'F-D0047-019']

    # mydb = pymysql.connect(host='34.81.77.216',post=3306,user=mysql_username,password=mysql_password,db=database,charset="utf8")


    engine = create_engine("mysql+pymysql://{}:{}@{}/{}".format(mysql_username, mysql_password, host_port, database))
    con = engine.connect()

    for i in range(len(location_name)):
        url = f'https://opendata.cwb.gov.tw/api/v1/rest/datastore/{codename[i]}?Authorization={authorization}&format=JSON&locationName={location_name[i]}'
        ss = requests.session()
        res = ss.get(url=url)
        time.sleep(5)
        j = json.loads(res.text)
        records = j['records']['locations'][0]['location'][0]
    #         print(records)
        location = records['locationName']
        col=['start_time', 'end_time', 'rain_probability(%)', 'temperature_avg', 
             'lowest_temperature', 'maximum_temperature', 'humidity(%)', 'weather']
        data = []
        location = records['locationName']
        print('location=', location)
        for i in range(len(records['weatherElement'][1]['time'])):
            start_time=records['weatherElement'][0]['time'][i]['startTime']
    #             print(start_time)
            end_time=records['weatherElement'][0]['time'][i]['endTime']
            rain=records['weatherElement'][0]['time'][i]['elementValue'][0]['value']
            if rain ==' ':
                rain = 'NaN'
            temp=records['weatherElement'][1]['time'][i]['elementValue'][0]['value']
            humidity=records['weatherElement'][2]['time'][i]['elementValue'][0]['value']
            weather=records['weatherElement'][6]['time'][i]['elementValue'][0]['value']
            low_temp=records['weatherElement'][8]['time'][i]['elementValue'][0]['value']
            high_temp=records['weatherElement'][12]['time'][i]['elementValue'][0]['value']
            data.append([start_time, end_time, rain, temp, low_temp, high_temp, humidity, weather])
        df=pd.DataFrame(data=data, columns=col)
        target_cols = ['temperature_avg', 'lowest_temperature', 'maximum_temperature', 'humidity(%)']
        df[target_cols] = df[target_cols].apply(pd.to_numeric)
        print(df)
        df.to_sql(name=f'weather_predict_{location}', con=con, if_exists='replace', index=False)
    con.close()
    return

def wether_today():
    engine = create_engine("mysql+pymysql://{}:{}@{}/{}".format(mysql_username, mysql_password, host_port, database))
    con = engine.connect()

    today = datetime.datetime.now()
    NowYear = today.year
    NowMonth = today.month
    NowDay = today.day

    Month = date_to_str(NowMonth)

    Error = []
    with open('./現存測站.csv',encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        column = [row for row in reader]

    for i in range(0,len(column)):
        col = ['ObsTime', 'StnPres', 'SeaPres', 'StnPresMax', 'StnPresMaxTime','StnPresMin',
           'StnPresMinTime', 'Temperature', 'TMax', 'TMaxTime', 'TMin', 'TMinTime',
           'Td dew point', 'RH', 'RHMin', 'RHMinTime', 'WS', 'WD', 'WSGust','WDGust',
           'WGustTime', 'Precp', 'PrecpHour', 'PrecpMax10', 'PrecpMax10Time', 'PrecpMax60',
           'PrecpMax60Time', 'SunShine', 'SunShineRate', 'GloblRad', 'VisbMean',
           'EvapA', 'UVIMax', 'UVIMaxTime', 'CloudAmount']

        StationNumber = column[i]['站號']
        StationName = column[i]['站名']
        StationLocation = column[i]['城市']
        Remark = column[i]['備註']

        if Remark == "本站只有雷達觀測資料。":
            print('{} only have radar observation data'.format(StationName))

        else:
            print("==========================")
            print('Update {}'.format(StationName + "{}{}".format(NowYear, Month)))
            url = "http://e-service.cwb.gov.tw/HistoryDataQuery/MonthDataController.do?command=viewMain&station={}&stname=%25E9%259E%258D%25E9%2583%25A8&datepicker={}-{}".format(StationNumber, NowYear, Month)
            r = requests.get(url)
            r.encoding = "utf-8"
            soup = BeautifulSoup(r.text, "html.parser")
            tag_table = soup.find(id="MyTable")
            rows = tag_table.findAll("tr")
            rowList_org = [[cell.get_text().replace("\n", "").replace("\r", "") for cell in row.findAll(["td"])] for row in rows]

            # 去除前三行空值
            list(map(lambda x: rowList_org.pop(0), range(3)))
            rowList = []
            # 將所有欄位去除空白->“\xa0”
            for t in range(len(rowList_org)): # 天數
                rowList_t = list(map(lambda x: rowList_org[t][x].split('\xa0')[0], range(len(rowList_org[0])))) # 該天所有欄位
                rowList.append(rowList_t)
    #         try:
            df = pd.DataFrame(rowList,
                              columns=col)
            # 去除非數值日期
            df = df.drop(df.loc[(df['Temperature'].apply(lambda s: pd.to_numeric(s, errors='coerce')).notnull()==False)].index)
            # 新增年、月欄位
            df['Year'] = NowYear
            df['Month'] = NowMonth
            col.insert(0,'Month')
            col.insert(0,'Year')
            df = df[col]
            df['ObsTime'] = df['ObsTime'].apply(pd.to_numeric)
            df_read = pd.read_sql(f'weather_{StationName}', engine)
    #         print('df_read=', df_read)
            duplicate = pd.merge(df_read, df, how='inner')
    #         print('duplicate=', duplicate)
            for i in duplicate.index:
                df = df.drop(df.loc[(df['Year']==duplicate['Year'][i]) & (df['Month']==duplicate['Month'][i])].index[0])
            print('update = ',df)
            df.to_sql(name=(f'weather_{StationName}'), con=con, if_exists='append', index=False)
            time.sleep(1)
            print('Update {} is done'.format(StationName + "{}{}".format(NowYear, Month)))
            print("==========================")
            time.sleep(2) 

    return


today = datetime.datetime.now()
NowYear = today.year-1911
NowMonth = today.month
month_str = date_to_str(NowMonth)

# 市場價格 (種類, 開始日期(民國年/月/日))
marketing_price("guava", f'{NowYear}/{month_str}/01') 
marketing_price("emperorguava", f'{NowYear}/{month_str}/01') 
marketing_price("banana", f'{NowYear}/{month_str}/01')
marketing_price("scarletbanana", f'{NowYear}/{month_str}/01')


# 新聞 (開始年, 開始月, 結束年, 結束月)
coa_news(NowYear, NowMonth-1, NowYear, NowMonth)

# 新聞 (總爬取頁數)
afa_news(2) 

news_merge()

# 氣象資料(開始年分)
wether_today()

weather_predict()


