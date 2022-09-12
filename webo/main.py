import keyword
from this import d
import requests
import re
import os
from jsonpath import jsonpath
import pandas as pd
import datetime

def trans_time(v_str):
    GMT_FORMAT = '%a %b %d %H:%M:%S +0800 %Y'
    time_format = datetime.datetime.strptime(v_str,GMT_FORMAT)
    ret_time = time_format.strftime("%Y-%m-%d %H:%M:%S")
    return ret_time


def get_weibo_list(v_keyword,v_max_page):
    
    # 请求头
    headers = {
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36",
        "accept":"application/json, text/plain, */*",
        "accept-encoding":"gzip, deflate, br"
    }

    for page in range(1,v_max_page+1):

        print('==== 开始爬取第{}页微博 ===='.format(page))
        
        url = 'https://m.weibo.cn/api/container/getIndex'

        # 请求参数
        params = {
            "containerid":"100103type=1&q={}".format(v_keyword),
            "page_type":"searchall",
            "page":page
        }

        # 发起请求
        r = requests.get(url,headers=headers,params=params)

        print(r.status_code)
    
        # webo cards
        cards = r.json()["data"]["cards"]

        # pre webo texts
        pre_text_list = jsonpath(cards,"$..mblog.text")

        # post webo texts
        post_text_list = []

        # data re compile
        dr = re.compile(r'<[^>]+>',re.S)
        
        print('pre_text_list:',pre_text_list)

        if not pre_text_list:
            continue
        if type(pre_text_list) == list:
            for pre_text in pre_text_list:
                post_text = dr.sub('',pre_text)
                post_text_list.append(post_text)
        # date
        time_list = jsonpath(cards,'$..mblog.created_at')
        time_list = [trans_time(v_str=i) for i in time_list]
        
        # author
        author_list = jsonpath(cards,'$..mblog.user.screen_name')
        
        # id
        id_list = jsonpath(cards,'$..mblog.id')

        # bid
        bid_list=jsonpath(cards,'$..mblog.bid')

        # reposts
        reposts_count_list = jsonpath(cards,'$..mblog.reposts_count')

        # comments
        comments_count_list = jsonpath(cards,'$..mblog.comments_count')

        # attitudes
        attitudes_count_list = jsonpath(cards,'$..mblog.attitudes_count')

        df = pd.DataFrame(
            {
                '页码':[page] * len(id_list),
                '微博id':id_list,
                '微博bid':bid_list,
                'Author':author_list,
                'Post_time':time_list,
                'Content':post_text_list,
                'PostNum':reposts_count_list,
                'CommentNum':comments_count_list,
                'AttitudeNum':attitudes_count_list
            }
        )
        
        if os.path.exists(v_webo_file):
            header = None
        else:
            header = ['页码','微博id','微博bid','Author','Post_time','Content','PostNum','CommentNum','AttitudeNum']
        df.to_csv(v_webo_file,mode='a+',index=False,header=header,encoding='utf-8')
        print('==== Page {} finish ===='.format(page))

if __name__ == '__main__':
    starttime = datetime.datetime.now()
    print('>>>> start program!')

    # 页数
    max_search_page = 100
    
    # 关键字
    search_keyword = '补邮'

    # csv文件
    v_webo_file = 'webo_{}_total_num_{}.csv'.format(search_keyword,max_search_page)
    
    print('>>>> crawl total {} for keyword {}'.format(max_search_page,search_keyword))

    # pre clean job
    if os.path.exists(v_webo_file):
        os.remove(v_webo_file)
        print('>>>> clean job done')

    # get data
    get_weibo_list(v_keyword=search_keyword,v_max_page=max_search_page)

    # data duplicates clean 
    df = pd.read_csv(v_webo_file)
    # 
    df.drop_duplicates(subset=['微博bid'],inplace=True,keep='first')
    df.to_csv(v_webo_file,index=False,encoding='utf_8_sig')

    # data filter
    df_filter = pd.read_csv(v_webo_file)
    # 转发数
    df_filter = df[df['PostNum']>=100]
    # 微博内容
    df_filter = df_filter[df_filter["Content"].str.contains(search_keyword)]
    # 微博作者
    df_filter = df_filter[df_filter["Author"].str.contains(search_keyword)]
    # 发表时间
    df_filter = df_filter[df_filter["Post_time"]>="2022-09-06 23:09:00"]

    df_filter.to_csv(v_webo_file,index=False,encoding='utf_8_sig')
    endtime = datetime.datetime.now()
    print('>>>> program finish! cost {} seconds'.format((endtime - starttime).seconds))


