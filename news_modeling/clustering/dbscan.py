import threading
import pandas as pd
import numpy as np
from datetime import datetime
from tqdm import tqdm

from pororo import Pororo
from sklearn.cluster import DBSCAN
from sklearn import metrics
from sklearn.datasets import make_blobs
from sklearn.preprocessing import StandardScaler
from sklearn.metrics.pairwise import cosine_distances
# from word2vec.model import WordToVector


class DBSCAN_news:
    def __init__(self, data: list, thread=8, eps=0.15, already_vectorized=False):
        """
        info : DBSCAN 객체 생성시 데이터를 넣으면 데이터프레임 형태로 조회 가능.
        input : news_data list : return of query ( SELECT id, article_headline, keywords FROM news )
        """

        if data is None:
            raise AttributeError()

        # create WordToVector obj to vertorize news
        # self.wv = WordToVector()

        # create dataframe to use sklearn DBSCAN
        if already_vectorized:

            for temp in data:

                for col, element in enumerate(temp["vector"]):
                    temp[str(col)] = element
            
            df = pd.DataFrame(data=data)
            df = df.set_index("id")
        else:
            df = self._set_df_thread_pool(data, thread)
            # df = self._set_df(data, thread)

        # train set
        X = df[[str(i) for i in range(768)]]
        dbscan = DBSCAN(eps=eps, min_samples=2, metric="cosine").fit(X)
        core_samples_mask = np.zeros_like(dbscan.labels_, dtype=bool)
        core_samples_mask[dbscan.core_sample_indices_] = True
        labels = dbscan.labels_

        # clustering info
        self.n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
        self.n_noise_ = list(labels).count(-1)

        # labeling
        tmp_ = self.n_clusters_
        for index, cluster in enumerate(labels):
            if cluster == -1:
                labels[index] = tmp_
                tmp_ += 1

        # save dataframe data in class variable
        df["clusters"] = labels
        self.dataframe = df[["article_headline", "keywords", "clusters", "created_datetime", "article_context", "article_url", "category", "image_url", "vector"]]
        

    def info(self):
        """
        return : clustering info of DBSCAN
        """

        # print("Estimated number of clusters: %d" % self.n_clusters_)
        # print("Estimated number of noise points: %d" % self.n_noise_)

        return {"cluster_num": self.n_clusters_, "noise_num": self.n_noise_}

    def save_csv(self, path: str):
        """
        input : save path
        """
        path = path.strip("/")
        file = path + "/dbscan_results.csv"

        self.dataframe.sort_values(by="clusters").to_csv(file, encoding="utf-8-sig")
    
    def __vectorize_pororo_split(self, pororo_instance, article : str):
        article = article.split(". ")
        vector = sum(list(map(pororo_instance, article)))

        return vector

    def __vectorize_pororo(self, pororo_instance, article : str):
        vector = pororo_instance(article)

        return vector
    
    def __parallel_vectorize(self, news_rows, total_data, lock):
        data = []
        se = Pororo(task="sentence_embedding", lang="ko")
        for row in tqdm(news_rows):
            temp = dict()
            try:
                # default vectorization by wordTobvector model
                # keywords_list = row["keywords"].lstrip("{").rstrip("}").split(",")
                # vector = self.wv.vectorize_word_list(keywords_list)

                # vectorization by Pororo model
                vector = self.__vectorize_pororo_split(se, row["article_context"])
                # vector = self._vectorize_pororo(row["article_context"])

                # start = datetime.now()
                # end = datetime.now()
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

    def _set_df(self, news_rows: list, thread: int):
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

            tmp_thread = threading.Thread(target=self.__parallel_vectorize, args=(news_rows, data, lock, ), daemon=True)
            thread_list.append(tmp_thread)
            tmp_thread.start()
        
        for th in thread_list:
            th.join()

        # 데이터 프레임으로 변환
        df = pd.DataFrame(data=data)
        df = df.set_index("id")

        return df
    

    def _set_df_thread_pool(self, news_rows: list, thread: int):
        """
        input : news_data list : return of query ( SELECT id, article_headline, keywords FROM news )
        """

        # 데이터를 데이터프레임 형식으로 변환하기 위해 news의 대표 벡터를 뽑아 flat 하는 과정
        data = list()

        # thread 관련
        thread_list = []
        lock = threading.Lock();

        for i in range(thread):

            tmp_thread = threading.Thread(target=self.__parallel_vectorize_thread_pool, args=(news_rows, data, lock, ), daemon=True)
            thread_list.append(tmp_thread)
            tmp_thread.start()
        
        for th in thread_list:
            th.join();

        # 데이터 프레임으로 변환
        df = pd.DataFrame(data=data)
        df = df.set_index("id")

        return df
    
    def __parallel_vectorize_thread_pool(self, news_rows, total_data, lock):
        data = []
        se = Pororo(task="sentence_embedding", lang="ko")
        total_num = len(news_rows)

        while True:

            num_data = len(news_rows)
            if num_data == 0:
                break
            else:
                print(f"{num_data:>6} / {total_num:>6}\r", end="")
            
            lock.acquire()
            row = news_rows.pop()
            lock.release()

            temp = dict()

            try:
                # default vectorization by wordTobvector model
                # keywords_list = row["keywords"].lstrip("{").rstrip("}").split(",")
                # vector = self.wv.vectorize_word_list(keywords_list)

                # vectorization by Pororo model
                vector = self.__vectorize_pororo_split(se, row["article_context"])
                # vector = self._vectorize_pororo(row["article_context"])

                # start = datetime.now()
                # end = datetime.now()
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
