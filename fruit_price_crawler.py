"""
CFB101 independent study fruit price prediction.
co-editing by Adan, Yuting, Esther, 郁棻, 修哥
version: 1.0
"""

# define
from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
import json
import os
import time
import datetime
import numpy as np
from selenium.webdriver import Chrome
from selenium.webdriver.support.ui import Select
import jieba
import jieba.analyse
from pymongo import MongoClient

ss = requests.session()


# Text
def title_mining(tmp_t):
    return jieba.analyse.extract_tags(tmp_t, topK=4, withWeight=True, allowPOS=('n', 'nr', 'ns', 'nz', 'v', 'vd', 'vn'))


def content_mining(tmp_c):
    return jieba.analyse.extract_tags(tmp_c, topK=20, withWeight=True, allowPOS=('n', 'nr', 'ns', 'nz', 'v', 'vd', 'vn'))


def news_jieba(function_name, ID_jieba, title_jieba, content_jieba):
    content_list = []
    title_list = []

    # 因為有不同文章來源，為了區別使用爬蟲網站縮寫+_id, coa=農委會, afa=農糧署
    title_col = [function_name + '_id', 'key_1', 'value_1', 'key_2', 'value_2', 'key_3', 'value_3', 'key_4', 'value_4']
    content_col = [function_name + '_id', 'key_1', 'value_1', 'key_2', 'value_2', 'key_3', 'value_3', 'key_4',
                   'value_4', 'key_5', 'value_5',
                   'key_6', 'value_6', 'key_7', 'value_7', 'key_8', 'value_8', 'key_9', 'value_9', 'key_10', 'value_10',
                   'key_11', 'value_11', 'key_12', 'value_12', 'key_13', 'value_13', 'key_14', 'value_14', 'key_15',
                   'value_15',
                   'key_16', 'value_16', 'key_17', 'value_17', 'key_18', 'value_18', 'key_19', 'value_19', 'key_20',
                   'value_20']

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
                title_keyword.append(tmp_title[i][0])  # 分詞 str
                title_keyword.append(tmp_title[i][1])  # 詞頻 float

        # 若內文找到的關鍵字小於20個(topK=20)則則key填入"NA",value填0,若這邊是空值會導致dataframe columns長度不符
        for i in range(20):
            # 內容
            if i >= len(tmp_content):
                title_keyword.append('NA')
                title_keyword.append(0)
            else:
                content_keyword.append(tmp_content[i][0])  # 分詞 str
                content_keyword.append(tmp_content[i][1])  # 詞頻 float
        # 此篇文章處理完就加入list中 -> [[文章1],[文章2]...]
        title_list += [title_keyword]
        content_list += [content_keyword]

    # 建立DataFrame並return
    df_title = pd.DataFrame(np.array(title_list), columns=title_col)
    df_content = pd.DataFrame(np.array(content_list), columns=content_col)

    return (df_title, df_content)


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
        try:  # text會取到"發布日期：110-06-02"，後面再用split切割成 ["發布日期：", "110-06-02"]，取第二個
            post_date = \
            sub_soup.select('div[class="agricultural-news-content-title row mb-lg"]')[0].text.split('發布日期：')[1]
        except:
            post_date = 'post_date Error'

        # return只會傳str，需要將上面取得之內容放進list內整個回傳， 否則會只回傳第一個字元
        tmp_list = [re.sub('[-:_、【】。；：)(「」，.&+\n\t\r\u3000]', ' ', title_t),
                    re.sub('[-:_、【】。；：)(「」，.&+\n\t\r\u3000]', ' ', content_t),
                    re.sub('-', '/', post_date)]
        return tmp_list[0], tmp_list[1], tmp_list[2]

    url = []
    link = []
    out = []
    title = []
    content = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
        }
    org_url = 'https://www.afa.gov.tw/cht/index.php?code=list&ids=307'
    page_tmp = 1
    for i in range(1, page_tmp + 1):
        url.append(org_url + '&page={}'.format(i))

    # 從主頁面get request並用BeautifulSoup轉換成html，用開發工具發現子頁面的連結在a標籤的'article_class'屬性中
    # 存取該標籤的'href'，並存到link中
    for page in range(len(url)):
        print('page=', page + 1)
        res = ss.get(url=url[page], headers=headers)
        soup = BeautifulSoup(res.text, 'html.parser')

        # 對照圖(一)農業新聞主頁面的開發者工具
        # 把要爬的所有子頁面的連結都先存起來
        for i in range(0, len(soup.select('a[class="article_class"]'))):
            link.append(soup.select('a[class="article_class"]')[i]['href'])
            res_sub = ss.get(url=link[i], headers=headers)
            soup_sub = BeautifulSoup(res_sub.text, 'html.parser')
            #  print('link=', link)

    # 將link中的article_id當成存進資料庫後的唯一識別
    ID = list(map(lambda x: x.split('&article_id=')[1], link))
    # print('ID=', ID)

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
    client = MongoClient()
    db = client.test
    afa_news_title_jieba = db.afa_news_title_jieba
    afa_news_content_jieba = db.afa_news_content_jieba
    afa_news = db.afa_news

    # 定義數字型態的columns並將該欄位所有rows轉換成數字型態
    news_col = ['afa_id', 'date', 'title', 'content', 'link']
    title_cols = ['afa_id', 'value_1', 'value_2', 'value_3', 'value_4']
    content_cols = ['afa_id', 'value_1', 'value_2', 'value_3', 'value_4', 'value_5',
                    'value_6', 'value_7', 'value_8', 'value_9', 'value_10',
                    'value_11', 'value_12', 'value_13', 'value_14', 'value_15',
                    'value_16', 'value_17', 'value_18', 'value_19', 'value_20']
    df_afa_news = pd.DataFrame(np.array(out), columns=news_col)

    df_title[title_cols] = df_title[title_cols].apply(pd.to_numeric)
    df_content[content_cols] = df_content[content_cols].apply(pd.to_numeric)
    df_afa_news['afa_id'] = df_afa_news['afa_id'].apply(pd.to_numeric)

    print("afa_news update to mongodb -> start")
    # 判斷id是否存在於mongodb中，若無則寫進資料庫
    for excist_id in df_afa_news['afa_id']:
        if [x for x in afa_news.find({"afa_id": int(excist_id)})] == []:
            afa_news_update = df_afa_news.loc[df_afa_news["afa_id"] == excist_id].to_dict(orient='records')
            updated = afa_news.insert_one(afa_news_update[0]).inserted_id
            print("afa_news update id ", updated)

    for excist_id in df_title['afa_id']:
        if [x for x in afa_news_title_jieba.find({"afa_id": int(excist_id)})] == []:
            afa_news_title_jieba_update = df_title.loc[df_title["afa_id"] == excist_id].to_dict(orient='records')
            # print(afa_news_title_jieba_update[0])
            updated = afa_news_title_jieba.insert_one(afa_news_title_jieba_update[0]).inserted_id
            print("afa_news title jieba update id ", updated)

    for excist_id in df_content['afa_id']:
        if [x for x in afa_news_content_jieba.find({"afa_id": int(excist_id)})] == []:
            afa_news_content_jieba_update = df_content.loc[df_content["afa_id"] == excist_id].to_dict(orient='records')
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
        date.append(newsDate.text)

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
    print('out=', out)
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
    client = MongoClient()
    db = client.test
    coa_news_title_jieba = db.coa_news_title_jieba
    coa_news_content_jieba = db.coa_news_content_jieba
    coa_news = db.coa_news


    df_title[title_cols] = df_title[title_cols].apply(pd.to_numeric)
    df_content[content_cols] = df_content[content_cols].apply(pd.to_numeric)


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


# data
def get_typhoon_alart():
    print("typhoon data crawler -> start")
    header_typhoon = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
        "Cookie": "PHPSESSID=ck51k725hgodfmfd7cbi4b0ks4; _gid=GA1.3.1183769150.1622787669; _ga=GA1.3.25450972.1620709885; _ga_K6HENP0XVS=GS1.1.1622787669.3.1.1622787695.0; TS01b0fe7f=0107dddfefcf72bbe6298d9f6067078ff9f4c14164221cb96410f497cf4481230f20f5073ca7ae71a4a5fe265de60a5c20c91db504",
        "X-Requested-With": "XMLHttpRequest"}
    url_typhoon = "https://rdc28.cwb.gov.tw/TDB/public/warning_typhoon_list/get_warning_typhoon"
    cols_typhoon = ['颱風編號',
                    '中文名稱',
                    '英文名稱',
                    '侵臺路徑分類',
                    '海上警報開始時間',
                    '近臺強度',
                    '近臺最低氣壓(hPa)',
                    '近臺最大風速(m/s)',
                    '近臺7級風暴風半徑(km)',
                    '近臺10級風暴風半徑(km)',
                    '海上警報結束時間',
                    '警報發布報數']

    res = ss.post(url_typhoon, headers=header_typhoon)
    data = res.text[1:]
    json_data = json.loads(data)
    df = pd.json_normalize(json_data)
    #     print(df)

    print("typhoon data crawler -> finish")
    client = MongoClient()
    db = client.test
    typhoon = db.typhoon

    print("typhoon data update to mongodb -> start")
    for excist_id in df['id']:
        if [x for x in typhoon.find({'id': int(excist_id)})] == []:
            typhoon_update = df.loc[df["id"] == excist_id].to_dict(orient='records')
            #         print(typhoon_update[0])
            updated = typhoon.insert_one(typhoon_update[0]).inserted_id
            print("typhoon data update id ", updated)

    print("typhoon data update to mongodb -> finish")
    client.close()


def produce_year_data():
    print('produce_year_data crawler start')
    url = "https://data.coa.gov.tw/Service/OpenData/DataFileService.aspx?UnitId=135"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
    }

    res = requests.get(url, headers=headers)
    data = json.loads(res.text)
    df = pd.json_normalize(data)  # normalize => 將json檔案攤平，如有巢狀結構的話
    print('produce_year_data crawler finish')

    wanted_col = ['年度', '地區別', '果品類別', '收穫株數', '收穫面積_公頃', '產量_公噸']
    df = df[wanted_col]
    df = df.loc[~(df["地區別"].isin(["臺灣省", "福建省"]))]

    # 去除含有字串的資料
    target_col = ['收穫株數', '收穫面積_公頃', '產量_公噸']
    df.loc[(df["收穫面積_公頃"].str.contains(r"[A-Z-]", na=False)), target_col] = 0
    df[target_col] = df[target_col].apply(pd.to_numeric)

    client = MongoClient()
    db = client.test
    fruit_produce_year = db.fruit_produce_year

    print("produce_year data update to mongodb -> start")
    fruit_produce_year_update = df.to_dict(orient='records')
    updated = fruit_produce_year.insert_many(fruit_produce_year_update)

    print("produce_year data update to mongodb -> finish")
    client.close()
    return


def origin_price(StartYear_tmp, StartMonth_tmp, EndYear_tmp, EndMonth_tmp):
    print('origin_price crawler -> start')
    url = "https://apis.afa.gov.tw/pagepub/AppContentPage.aspx?itemNo=PRI075"
    driver = Chrome('./chromedriver')
    driver.get(url)

    driver.find_element_by_xpath("//*[@id='PRI105']").click()
    time.sleep(5)
    ###選旬平均
    driver.find_element_by_xpath("//*[@id='WR1_2_Q_AvgPriceType_C1_2']").click()
    time.sleep(3)
    ###開始年
    select = Select(driver.find_element_by_name('WR1_2$Q_PRSR_Year1$C1'))
    select.select_by_visible_text(u"{}".format(StartYear_tmp))
    ###開始月
    time.sleep(1)
    select = Select(driver.find_element_by_name('WR1_2$Q_PRSR_Month1$C1'))
    select.select_by_visible_text(u"{}".format(StartMonth_tmp))
    ###結束年
    time.sleep(1)
    select = Select(driver.find_element_by_name('WR1_2$Q_PRSR_Year2$C1'))
    select.select_by_visible_text(u"{}".format(EndYear_tmp))
    ###結束月
    time.sleep(1)
    select = Select(driver.find_element_by_name('WR1_2$Q_PRSR_Month2$C1'))
    select.select_by_visible_text(u"{}".format(EndMonth_tmp))
    ###選種類
    time.sleep(1)
    driver.find_element_by_xpath("//input[@id='WR1_2_PRMG_02_23']").click()
    ###查詢
    time.sleep(1)
    driver.find_element_by_xpath("//*[@class='CSS_ABS_NormalLink']").click()

    time.sleep(2)
    window_1 = driver.current_window_handle
    windows = driver.window_handles
    for current_window in windows:
        if current_window != window_1:
            driver.switch_to.window(current_window)

    time.sleep(2)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    try:
        table = soup.select("table[border='1']")
    except:
        print('production price Error')

    rows = table[0].findAll("tr")
    rowList = [[cell.get_text().replace("\n", "") for cell in row.findAll(["td"])] for row in rows]
    Header = rowList[0]
    Content = rowList[1:len(rowList)]
    df = pd.DataFrame(Content, columns=Header)
    print('origin_price crawler -> finish')

    # 初步資料清潔
    df_transposed = df.set_index('地點').T
    df_transposed = df_transposed.reset_index()
    df_transposed = df_transposed.rename(columns={"index": "時間"})

    col = []
    for i in range(1, len(df_transposed.columns)):
        col.append(df_transposed.columns[i])

    df_transposed[df_transposed[col] == '-'] = 0
    df_transposed[col] = df_transposed[col].apply(pd.to_numeric)
    # 清掉->(元/公斤)
    df_transposed["時間"] = df_transposed["時間"].apply(lambda x: x.split("(")[0])
    print(df_transposed.head())

    print("origin_price update to mongodb -> start")
    client = MongoClient()
    db = client.test
    origin_price_data = db.origin_price_data

    for excist_time in df_transposed['時間']:
        if [x for x in origin_price_data.find({"時間": excist_time})] == []:
            origin_price_data_update = df_transposed.loc[df_transposed["時間"] == excist_time].to_dict(orient='records')
            updated = origin_price_data.insert_one(origin_price_data_update[0]).inserted_id
            print("origin_price_data update id ", updated)
    client.close()
    print("origin_price update to mongodb -> finish")


def marketing_price_soup(soup):
    cnt = 0
    table = soup.select("table[border='1']")
    table_content = list(filter(None, table[0].text.split('\n')))
    Header = table_content[0:10]
    Header[7] = '價格跟前一交易日比較%'
    Header[9] = '交易量跟前一交易日比較%'
    print('marketing_price crawler -> finish')

    # 清洗資料
    data = table_content[18:]
    output = []
    for s_data in range(0, len(data), 10):
        output.append(data[s_data:s_data + 10])
    df3 = pd.DataFrame(output, columns=Header)
    # 按日期排序，reset_index後會多出index欄位，後面讀回的原始資料沒有index因此一併去除
    df3 = df3.sort_values(["日期"], ascending=True).reset_index().drop(['index'], axis=1)

    # 將交易量中的","去除並轉成數字
    df3["交易量(公斤)"] = df3["交易量(公斤)"].apply(lambda x: int(" ".join(re.sub(",", "", x).split())))

    # dataFrame部分欄位轉成數字型態
    col = ["上價", "中價", "下價", "平均價(元/公斤)", "交易量(公斤)"]
    df3[col] = df3[col].apply(pd.to_numeric)

    print("marketing_price update to mongodb -> start")
    # 寫進mongodb
    client = MongoClient()
    db = client.test
    marketing_price_data = db.marketing_price_data

    # 用日期與市場欄位判斷資料是否已存在，若無則寫入資料庫
    for excist_time, excist_marketing in zip(df3['日期'], df3['市場']):
        #         print(excist_time,excist_marketing)
        if [x for x in marketing_price_data.find({"日期": excist_time, "市場": excist_marketing})] == []:
            marketing_price_data_update = df3.loc[(df3["日期"] == excist_time) & (df3["市場"] == excist_marketing)].to_dict(
                orient='records')
            updated = marketing_price_data.insert_one(marketing_price_data_update[0]).inserted_id
            print("marketing_price_data update id ", updated)
    print("marketing_price update to mongodb -> finish")

    return


def marketing_price(fruit, start_date):
    print('marketing_price crawler -> start')
    fruit_select = {"香蕉": "A1", "鳳梨": "B2"}
    url = "https://amis.afa.gov.tw/fruit/FruitProdDayTransInfo.aspx"

    driver = Chrome("./chromedriver")
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
    # time.sleep(3)

    # 點選後轉移到跳出視窗，選取全部市場
    iframe = driver.find_elements_by_tag_name("iframe")[0]
    driver.switch_to.frame(iframe)
    radio_target = driver.find_element_by_xpath("//*[@id='radlMarketRange_0']")
    radio_target.click()

    #### 4. 選取水果種類
    driver.find_element_by_xpath("//*[@id='ctl00_contentPlaceHolder_txtProduct']").click()
    # time.sleep(3)

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

    time.sleep(3)
    #     html = driver.execute_script("return document.getElementsByTagName('html')[0].outerHTML")

    soup = BeautifulSoup(driver.page_source, 'html.parser')

    marketing_price_soup(soup)


def agriculture_survey():
    print("agriculture_survey crawler start")
    url = "https://data.coa.gov.tw/Service/OpenData/FromM/TownCropData.aspx"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
            }

    res = requests.get(url, headers=headers)
    data = json.loads(res.text)
    df = pd.json_normalize(data)
    print("agriculture_survey crawler finish")

    print("agriculture_survey update to mongodb -> start")
    client = MongoClient()
    db = client.test
    agriculture_survey_data = db.agriculture_survey_data

    agriculture_survey_update = df.to_dict(orient='records')
    agriculture_survey_data.insert_many(agriculture_survey_update)
    print("agriculture_survey update to mongodb -> finish")


def Fruit_season():
    print("Fruit_season crawler start")
    url = "https://data.coa.gov.tw/Service/OpenData/DataFileService.aspx?UnitId=061&$top=6000&$skip=0"  # "https://data.coa.gov.tw/Service/OpenData/DataFileService.aspx?UnitId=113" #"https://data.coa.gov.tw/Service/OpenData/FromM/TownCropData.aspx"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
    }

    res = requests.get(url, headers=headers)
    data = json.loads(res.text)
    print("Fruit_season crawler finish")

    df = pd.json_normalize(data)
    df_fruit = df.loc[df["type"] == '水果']
    df_fruit["month"] = df_fruit["month"].astype("int")

    print("Fruit_season update to mongodb -> start")
    client = MongoClient()
    db = client.test
    Fruit_season_data = db.Fruit_season_data

    df_fruit_data_update = df_fruit.to_dict(orient='records')
    Fruit_season_data.insert_many(df_fruit_data_update)
    print("Fruit_season update to mongodb -> finish")


def main():

    # 市場價格 (種類, 開始日期(民國年/月/日))
    marketing_price("香蕉", '110/05/01')

    # 產地價格
    origin_price(2020, 1, 2020, 12)

    # 新聞 (開始年, 開始月, 結束年, 結束月)
    coa_news(110, 4, 110, 6)

    # 新聞 (總爬取頁數)
    afa_news(2)

    # 年度生產愾況
    produce_year_data()

    # 氣象資料(開始年分)
    # wether(2021)

    # 水果產季
    Fruit_season()

    # 農情調查
    agriculture_survey()

    # 颱風警報
    get_typhoon_alart()


if __name__ == '__main__':
    main()


