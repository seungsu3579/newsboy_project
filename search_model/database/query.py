# -*- coding: utf-8 -*- 

from .connection import *

def select_all():
    try:
        conn = connect()
        # 딕셔너리 형태로 반환
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # 추후 로직 변경해야함 시간단위로 Where 쪼개야함 현재시간 기준 -1시간
        sql = "SELECT id, article_context FROM news WHERE article_context IS NOT NULL"
        cur.execute(sql)
        array = cur.fetchall()
        return array
    except Exception as e:
        raise e

def select_keyword():
    try:
        conn = connect()
        # 딕셔너리 형태로 반환
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # 추후 로직 변경해야함 시간단위로 Where 쪼개야함 현재시간 기준 -1시간
        sql = "SELECT id, keywords FROM news WHERE keywords IS NOT NULL"
        cur.execute(sql)
        array = cur.fetchall()
        return array
    except Exception as e:
        raise e



def update_keyword_keysentence(news_id, keyword):
    try:
        conn = connect()
        # 딕셔너리 형태로 반환
        cur = conn.cursor()
        # TODO 추후 로직 변경해야함 시간단위로 Where 쪼개야함 현재시간 기준 -1시간
        sql = "UPDATE news SET keywords = %s where id = %s"
        cur.execute(sql, (keyword, news_id,))
        conn.commit()

    except Exception as e:
        print(e)
        raise e

def get_keywords_by_time(timestamp, time_interval):
    """
    input :
        timestamp : 기준 시간
        time_interval: 기준 시간으로부터 유효 시간
    return : 
        해당 시간에 맞는 keyword list
    """

    try:
        conn = connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        sql = "SELECT id, article_headline, keywords, cluster_id FROM news WHERE created_datetime between %s and %s AND keywords is not null;"
        cur.execute(sql, ((timestamp-time_interval).strftime('%Y-%m-%d %H:%M:%S'), timestamp.strftime('%Y-%m-%d %H:%M:%S'), ))
        array = cur.fetchall()
        array = [{
            "id" : row[0],
            "article_headline" : row[1],
            "keywords" : row[2],
            "cluster_id" : row[3]} for row in array
        ]
        return array
    except Exception as e:
        print(e)
        raise e

def get_keywords_by_clusterid(cluster_ids:list):
    """
    input :
        cluster_ids : 클러스터 id 리스트
    return :
        cluster_id 리스트에 해당되는 모든 news의 keyword list
    """

    try:
        conn = connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        sql = f"SELECT id, article_headline, keywords, cluster_id FROM news WHERE cluster_id IN ({str(cluster_ids).strip('[]')}) AND keywords is not null"
        cur.execute(sql)
        array = cur.fetchall()
        array = [{
            "id" : row[0],
            "article_headline" : row[1],
            "keywords" : row[2],
            "cluster_id" : row[3]} for row in array
        ]
        return array
    except Exception as e:
        print(e)
        raise e
    





# def newly_inserted_list():
#     """
#     return :
#         newly inserted news list
#     function :
#         새롭게 keyword analysis 해야 할 리스트
#     """
#     sql = "SELECT nm.news_id, date(nm.created_datetime), nm.category \
#         FROM news_metadata nm LEFT OUTER JOIN news_extracted ne \
#         ON ne.news_id = nm.news_id WHERE ne.keyword IS NULL"

#     return queryall(sql)


# def get_keyword_keysentence(news_id):

#     """
#     input :
#         news_id : 뉴스 아이디
#     return :
#         keyword, keysentence info
#     function :
#         중요 키워드와 문장을 리턴
#     """
#     sql = "SELECT * FROM news_extracted WHERE news_id = %s"

#     return queryone(sql, (news_id,))


# def get_preprocessed_news_ids():

#     """
#     input :
#         news_id : 뉴스 아이디
#     return :
#         keyword, keysentence info
#     function :
#         중요 키워드와 문장을 리턴
#     """
#     sql = "SELECT * FROM news_extracted"

#     return queryall(sql)


# def get_header(news_id):

#     sql = "SELECT ne.news_id, nm.article_headline, ne.keyword, ne.key_sentence FROM (SELECT news_id, keyword, key_sentence FROM news_extracted WHERE news_id = %s) AS ne\
#             LEFT JOIN news_metadata nm on ne.news_id = nm.news_id"

#     return queryone(sql, (news_id,))


# def select_not_null(category, date):
#     sql = "SELECT news_id,key_sentence, keyword, date(created_datetime) as date from news_extracted \
#         where key_sentence != '' and category = %s and date(created_datetime) = %s;"
#     return queryall(sql, (category, date, ))


# def get_current_articles(category, num):

#     sql = "SELECT news_id,key_sentence, keyword, date(created_datetime) as date FROM news_extracted WHERE category = %s and key_sentence <> '' ORDER BY created_datetime DESC LIMIT %s;"
#     return queryall(sql, (category, num, ))

# def insert_representatives(news_id):
#     sql = "INSERT INTO news_service (created_datetime, image_url, article_headline, article_url, category, news_id) \
#         SELECT created_datetime, image_url, article_headline, article_url, category, news_id FROM news_metadata where news_id = %s;"
#     return execute(sql, (news_id, ))

# def update_representatives(news_id):
#     sql = "UPDATE news_service SET key_sentence = (SELECT key_sentence FROM news_extracted WHERE news_id = %s) where news_id = %s;"
#     return execute(sql, (news_id, news_id ))
