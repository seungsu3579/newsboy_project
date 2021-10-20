import requests
import pickle
import threading
import pandas as pd
import numpy as np
import datetime
from tqdm import tqdm
import unicodedata as unicode
import json

from pororo import Pororo
from elasticsearch import Elasticsearch

from error import *
from database.controller import DBController, ServiceDBController


class DataLoader:

    def __init__(cls, *args, **kwargs):
        super(CLASS_NAME, cls).__init__(*args, **kwargs)
    
    @staticmethod
    def save_object_as_pickle(object, filename):
        
        try:
            with open(filename, 'wb') as file:
                pickle.dump(object, file)
            return True
        except:
            return False

    @staticmethod
    def load_pickle_as_object(filename):
        
        return_value = None
        with open(filename, 'rb') as file:
            return_value = pickle.load(file)
        
        return return_value
    
    @staticmethod
    def load_recent_news_data(now, interval):

        db = DBController()
        recent_news_data = dict()
        interval_obj = datetime.timedelta(minutes=interval)
        recent_news_tmp = db.get_article_by_realtime(now, interval_obj)
        print(len(recent_news_tmp))
        if len(recent_news_tmp) != 0:
            for news in recent_news_tmp:
                if len(news['article_context'].split('. ')) > 8:
                    recent_news_data[news['id']] = dict(news)
        else:
            raise NoRecentNewsDataError
        
        return recent_news_data

    

class Vectorizer:

    def __init__(self, *args, **kwargs):
        super(CLASS_NAME, self).__init__(*args, **kwargs)

    @classmethod
    def __vectorize_pororo_split(cls, pororo_instance, article : str):
        article = article.split(". ")
        vector = sum(list(map(pororo_instance, article)))

        return vector

    @classmethod
    def __vectorize_pororo(cls, pororo_instance, article : str):
        vector = pororo_instance(article)

        return vector
    
    @classmethod
    def __parallel_vectorize(cls, news_rows, total_data, lock):
        data = []
        se = Pororo(task="sentence_embedding", lang="ko")
        for row in tqdm(news_rows):
            temp = dict()
            try:
                # default vectorization by wordTobvector model
                # keywords_list = row["keywords"].lstrip("{").rstrip("}").split(",")
                # vector = cls.wv.vectorize_word_list(keywords_list)

                # vectorization by Pororo model
                vector = cls.__vectorize_pororo_split(se, row["article_context"])
                # vector = cls._vectorize_pororo(row["article_context"])
                # print(f"{end - start} : {row['article_context']}")

            except Exception as e:
                print(e)
                raise e

            if type(vector) != np.ndarray:
                continue

            temp["id"] = row["id"]
            temp["keywords"] = row["keywords"]
            temp["article_headline"] = row["article_headline"]
            temp["created_datetime"] = row["created_datetime"]
            temp["article_context"] = row["article_context"]
            temp["article_url"] = row["article_url"]
            temp["category"] = row["category"]
            temp["image_url"] = row["image_url"]
            temp["vector"] = vector

            for col, element in enumerate(vector):
                temp[str(col)] = element
            data.append(temp)

        # lock
        lock.acquire()
        total_data += data
        lock.release()

        return 0

    @classmethod
    def set_df(cls, news_rows: list, thread: int):
        """
        input : news_data list : return of query ( SELECT id, article_headline, keywords FROM news )
        """

        # 데이터를 데이터프레임 형식으로 변환하기 위해 news의 대표 벡터를 뽑아 flat 하는 과정
        data = list()

        item_num = (len(news_rows) // thread) + 1
        parallel_news_list = [news_rows[i:i + item_num] for i in range(0, len(news_rows), item_num)]

        # thread 관련
        thread_list = []
        lock = threading.Lock();

        for news_rows in parallel_news_list:

            tmp_thread = threading.Thread(target=cls.__parallel_vectorize, args=(news_rows, data, lock, ), daemon=True)
            thread_list.append(tmp_thread)
            tmp_thread.start()
        
        for th in thread_list:
            th.join()

        # 데이터 프레임으로 변환
        df = pd.DataFrame(data=data)
        df = df.set_index("id")

        return df
    
    @classmethod
    def set_df_thread_pool(cls, news_rows: list, thread: int):
        """
        input : news_data list : return of query ( SELECT id, article_headline, keywords FROM news )
        """

        # 데이터를 데이터프레임 형식으로 변환하기 위해 news의 대표 벡터를 뽑아 flat 하는 과정
        data = dict()

        # thread 관련
        thread_list = []
        lock = threading.Lock();

        for i in range(thread):

            tmp_thread = threading.Thread(target=cls.__parallel_vectorize_thread_pool, args=(news_rows, data, lock, ), daemon=True)
            thread_list.append(tmp_thread)
            tmp_thread.start()
        
        for th in thread_list:
            th.join()

        # 데이터 프레임으로 변환
        # df = pd.DataFrame(data=data)
        # df = df.set_index("id")

        # return df
        return data
    
    @classmethod
    def __parallel_vectorize_thread_pool(cls, news_rows, total_data, lock):
        data = dict()
        se = Pororo(task="sentence_embedding", lang="ko")
        total_num = len(news_rows)

        while True:

            num_data = len(news_rows)
            if num_data == 0:
                break
            else:
                print(f"{num_data:>6} / {total_num:>6}\r", end="")
            
            lock.acquire()
            if len(news_rows) != 0:
                row = news_rows.pop()
            lock.release()

            temp = dict()

            try:
                # default vectorization by wordTobvector model
                # keywords_list = row["keywords"].lstrip("{").rstrip("}").split(",")
                # vector = cls.wv.vectorize_word_list(keywords_list)

                # vectorization by Pororo model
                vector = cls.__vectorize_pororo_split(se, row["article_context"])
                # vector = cls._vectorize_pororo(row["article_context"])
                # print(f"{end - start} : {row['article_context']}")

            except Exception as e:
                print(e)
                raise e
            
            if type(vector) != np.ndarray:
                continue

            temp["id"] = row["id"]
            temp["keywords"] = row["keywords"]
            temp["article_headline"] = row["article_headline"]
            temp["created_datetime"] = row["created_datetime"]
            temp["article_context"] = row["article_context"]
            temp["article_url"] = row["article_url"]
            temp["category"] = row["category"]
            temp["image_url"] = row["image_url"]
            temp["vector"] = vector

            for col, element in enumerate(vector):
                temp[str(col)] = element
            data[row["id"]] = temp

        # lock
        lock.acquire()
        total_data.update(data)
        lock.release()

        return 0


class NewsReworker:

    def __init__(self, *args, **kwargs):
        self.summ = Pororo(task="summarization", model="extractive", lang="ko")
        self.ner = Pororo(task="ner", lang="ko")
        self.pos = Pororo(task="pos", lang="ko")
        self.es = Elasticsearch("http://search.newsboycorp.com:9200")
        self.tmp_dbs = ServiceDBController()
        self.STOPWORDS = ["한국일보", "매일경제", "일다", "이코노미스트", "조선일보", "국민일보", "MBN", "시사IN", "이데일리", "한경비즈니스", "서울신문", "월간 산", "더팩트", "헤럴드경제", "한국경제", "블로터", "SBS Biz", "뉴스1", "헬스조선", "디지털데일리", "YTN", "중앙SUNDAY", "코메디닷컴", "디지털타임스", "한겨레21", "노컷뉴스", "JTBC", "파이낸셜뉴스", "서울경제", "중앙일보", "세계일보", "매일신문", "TV조선", "동아일보", "기자협회보", "오마이뉴스", "한겨레", "매경이코노미", "경향신문", "한국경제TV", "여성신문", "강원일보", "코리아중앙데일리", "연합뉴스", "연합뉴스TV", "문화일보", "시사저널", "비즈니스워치", "신동아", "조세일보", "머니투데이", "레이디경향", "주간경향", "미디어오늘", "채널A", "부산일보", "프레시안", "동아사이언스", "코리아헤럴드", "ZDNet Korea", "SBS", "아이뉴스24", "뉴시스", "뉴스타파", "머니S", "데일리안", "전자신문", "조선비즈", "아시아경제", "MBC", "KBS", "주간조선", "주간동아"]

    def summarize_keyword(self, text_list : list, keyword_news_list : list, news_summary_list : list, lock, keyword_lock):
        
        for text in tqdm(text_list):

            news_dict = dict()

            ##### summarize #####
            try:
                summarized_text = self.summ(text.get("article_context"))
            except:
                article = text.get("article_context")
                summarized_text = ". ".join(article.split(". ")[:3])
                summarized_text += "...(후략)"
            
            ##### elastic search add #####
            id = text.get("id")
            doc = {'title' : text.get('article_headline'), 'summary' : summarized_text, 'article_url' : text.get('article_url'), 'image_url' : text.get('image_url'), "date": text.get("created_datetime")}

            lock.acquire()
            # TODO
            try:
                self.es.create(index="newsboy_news", id=id, body=doc)
            except:
                pass
            # print(f"ES INSERT : {doc}")
            lock.release()

            ##### extract keyword #####
            keywords = set()
            if len(summarized_text) > 512:
                summarized_text_keyword = summarized_text[:512]
            else:
                summarized_text_keyword = summarized_text
            
            try:
                ner_analysis = self.ner(summarized_text_keyword)
            except:
                ner_analysis = []
                # continue

            flag = True
            for word, tp in ner_analysis:
                if tp not in ['DATE', 'QUANTITY', 'O', 'COUNTRY', 'LOCATION', 'OCCUPATION', 'TIME', 'CITY', 'CIVILIZATION', 'TERM', 'ANIMAL'] and len(word) > 1:
                    if word.isalnum() and (word not in self.STOPWORDS):
                        # keywords.add((word, tp))
                        for c in word:
                            if 'HANGUL' not in unicode.name(c) and 'LATIN' not in unicode.name(c) and 'DIGIT' not in unicode.name(c):
                                flag = False
                                break
                        if flag:
                            keywords.add((word, tp))
                            flag = True
            
            for wordlist, tp in [(self.pos(word), tp) for word, tp in list(keywords)]: 
                if len(wordlist) == 1:
                    if len(wordlist[0]) == 0 or wordlist[0][1] != 'NNP':
                        keywords.remove((wordlist[0][0], tp))
            
            # upsert keyword on keyword table    
            upsert_keyword_list = []
            for keyword, tp in keywords:
                now = datetime.datetime.now() - datetime.timedelta(hours=7) - datetime.timedelta(minutes=41)
                now = now.strftime('%Y-%m-%d %H:%M:%S')
                upsert_keyword_list.append((now, now, keyword, tp, now, keyword,))
            

            # TODO
            keyword_lock.acquire()
            try:
                self.tmp_dbs.upsert_keyword(upsert_keyword_list)
            except:
                self.tmp_dbs = ServiceDBController()
                self.tmp_dbs.upsert_keyword(upsert_keyword_list)
            keyword_lock.release()
            # print(f"KEYWORD UPSERT : {upsert_keyword_list}")

            # save relation of keyword and article
            now = datetime.datetime.now() - datetime.timedelta(hours=7) - datetime.timedelta(minutes=41)
            now = now.strftime('%Y-%m-%d %H:%M:%S')
            keyword = list(map(lambda x : "\'" + x[0] + "\'", keywords))

            if len(keyword) != 0:
                
                try:
                    tmp_keyword_list = self.tmp_dbs.get_keyword_ids(", ".join(keyword))
                except:
                    self.tmp_dbs = ServiceDBController()
                    tmp_keyword_list = self.tmp_dbs.get_keyword_ids(", ".join(keyword))
                keyword_news_relation_list = [(now, now, row['keyword_id'], text.get("id"), now) for row in tmp_keyword_list] 
            else:
                keyword_news_relation_list = []

            news_dict['news_id'] = id
            news_dict['keywords'] = list(map(lambda x : x[0], keywords))
            news_dict['timestamp'] = now
            header = {'Content-Type': 'application/json; chearset=utf-8'}
            url = 'http://search.newsboycorp.com:8000/realtime/keywords_aggregate'
            res = requests.post(url, data=json.dumps(news_dict), headers=header)

            # lock
            lock.acquire()
            keyword_news_list += keyword_news_relation_list
            news_summary_list.append(
                (text.get("id"), text.get("created_datetime").strftime('%Y-%m-%d %H:%M:%S'), now, summarized_text, text.get("article_url"), 
                text.get("category"), text.get("article_headline"), text.get("image_url"), text.get("score"), now, text.get("score"), )
            )
            lock.release()


# util method

def cos_sim(A, B):
    return np.dot(A, B)/(np.linalg.norm(A)*np.linalg.norm(B))