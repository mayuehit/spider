import keyword
from this import d
import requests
import re
import os
from jsonpath import jsonpath
import pandas as pd
import datetime
import logging

# logger
console_handler = logging.StreamHandler()
console_handler.setLevel('DEBUG')
fmt = '%(asctime)s - %(funcName)s - %(lineno)s - %(levelname)s - %(message)s'
formatter = logging.Formatter(fmt)
console_handler.setFormatter(formatter)
logger = logging.getLogger('main')
# change level here if you want to see more
logger.setLevel("INFO")
logger.addHandler(console_handler)

def trans_time(v_str):
    GMT_FORMAT = '%a %b %d %H:%M:%S +0800 %Y'
    time_format = datetime.datetime.strptime(v_str, GMT_FORMAT)
    ret_time = time_format.strftime("%Y-%m-%d %H:%M:%S")
    return ret_time


def get_weibo_list(v_keyword, v_max_page):

    # 请求头
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36",
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br"
    }

    for page in range(1, v_max_page+1):

        logger.info('==== 开始爬取第{}页微博 ===='.format(page))

        url = 'https://m.weibo.cn/api/container/getIndex'

        # 请求参数
        params = {
            "containerid": "100103type=1&q={}".format(v_keyword),
            "page_type": "searchall",
            "page": page
        }
        # 发起请求
        r = requests.get(url, headers=headers, params=params)

        logger.info(r.status_code)

        # webo cards
        cards = r.json()["data"]["cards"]

        # pre webo texts
        pre_text_list = jsonpath(cards, "$..mblog.text")

        # post webo texts
        post_text_list = []

        # data re compile
        dr = re.compile(r'<[^>]+>', re.S)

        logger.debug('pre_text_list:')
        logger.debug(pre_text_list)

        if not pre_text_list:
            continue
        if type(pre_text_list) == list:
            for pre_text in pre_text_list:
                post_text = dr.sub('', pre_text)
                post_text_list.append(post_text)
                
        logger.debug('post_text_list:')
        logger.debug(post_text_list)
        # date
        time_list = jsonpath(cards, '$..mblog.created_at')
        time_list = [trans_time(v_str=i) for i in time_list]

        # author
        author_list = jsonpath(cards, '$..mblog.user.screen_name')

        # id
        id_list = jsonpath(cards, '$..mblog.id')

        # bid
        # bid_list = jsonpath(cards, '$..mblog.bid')

        # reposts
        reposts_count_list = jsonpath(cards, '$..mblog.reposts_count')

        # comments
        comments_count_list = jsonpath(cards, '$..mblog.comments_count')

        # attitudes
        attitudes_count_list = jsonpath(cards, '$..mblog.attitudes_count')

        logger.debug(f"pre_text_list_length:{len(pre_text_list)}\n"+
            f"post_text_list_length:{len(post_text_list)}\n"+
                f"time_list_length:{len(time_list)}\n"+
                    f"author_list_length:{len(author_list)}\n"+
                        f"id_list_length:{len(id_list)}\n"+
                            # f"bid_list_length:{len(bid_list)}\n"+
                                f"reposts_count_list_length:{len(reposts_count_list)}\n"+
                                    f"comments_count_list_length:{len(comments_count_list)}\n"+
                                        f"attitudes_count_list_length:{len(attitudes_count_list)}")

        df = pd.DataFrame(
            {
                '页码': [page] * len(id_list),
                '微博id': id_list,
                # '微博bid': bid_list,
                'Author': author_list,
                'Post_time': time_list,
                'Content': post_text_list,
                'PostNum': reposts_count_list,
                'CommentNum': comments_count_list,
                'AttitudeNum': attitudes_count_list
            }
        )

        if os.path.exists(v_webo_file):
            header = None
        else:
            header = ['页码', '微博id','Author', 'Post_time',
                      'Content', 'PostNum', 'CommentNum', 'AttitudeNum']
        df.to_csv(v_webo_file, mode='a+', index=False,
                  header=header, encoding='utf-8')
        logger.info('==== Page {} finish ===='.format(page))


if __name__ == '__main__':
    starttime = datetime.datetime.now()
    logger.info('>>>> start program!')

    # 页数
    max_search_page = 10

    # 关键字
    search_keyword = '雷峰塔'

    # 文本
    filter_content = ["", ""]

    # 作者
    filter_author = ""

    # 日期
    filter_time = ""

    # 转发数
    filter_posts_num = 0

    # 点赞数
    filter_attitude_num = 0

    # 评论数量
    filter_comments_num = 0

    # csv文件
    v_webo_file = 'webo_{}_total_num_{}.csv'.format(
        search_keyword, max_search_page)

    logger.info('>>>> crawl total {} for keyword {}'.format(
        max_search_page, search_keyword))

    # pre clean job
    if os.path.exists(v_webo_file):
        os.remove(v_webo_file)
        logger.info('>>>> clean job done')

    # get data
    get_weibo_list(v_keyword=search_keyword, v_max_page=max_search_page)

    # # data duplicates clean
    # df = pd.read_csv(v_webo_file)
    # df.drop_duplicates(subset=['微博bid'], inplace=True, keep='first')
    # df.to_csv(v_webo_file, index=False, encoding='utf_8_sig')

    # data filter
    df_filter = pd.read_csv(v_webo_file)
    # 转发数
    df_filter = df_filter[df_filter['PostNum'] >= filter_posts_num]
    # 评论数
    df_filter = df_filter[df_filter['CommentNum'] >= filter_comments_num]
    # 点赞数
    df_filter = df_filter[df_filter['AttitudeNum'] >= filter_attitude_num]
    # 微博内容
    for content in filter_content:
        df_filter = df_filter[df_filter["Content"].str.contains(content)]
    # 微博作者
    df_filter = df_filter[df_filter["Author"].str.contains(filter_author)]
    # 发表时间
    df_filter = df_filter[df_filter["Post_time"] >= filter_time]

    df_filter.to_csv(v_webo_file, index=False, encoding='utf_8_sig')
    endtime = datetime.datetime.now()
    logger.info('>>>> program finish! cost {} seconds'.format(
        (endtime - starttime).seconds))
