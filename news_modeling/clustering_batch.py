import os
import datetime
import numpy as np
import pandas as pd
import numpy as np
import time as timelib
from pororo import Pororo

from database.controller import DBController, ServiceDBController
from clustering.dbscan import DBSCAN_news

VECTOR_FILE_DIR = os.path.join(os.getcwd(), "newsboy_data")

def log(filename : str, text : str):
    log_f = open(filename, "a")
    print(text, file=log_f)
    log_f.close()


def cos_sim(A, B):
    return np.dot(A, B)/(np.linalg.norm(A)*np.linalg.norm(B))


def clustering(date, delta_hour : int = 1):
    ##### clustering option #####

    # max cluster size
    max_cnt = 300
    eps = 0.15

    ##############################

    # to connect db
    db = DBController()
    dbs = ServiceDBController()

    # get data by time
    file_date = date[:10]
    date = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')

    # interval = datetime.timedelta(days=delta_hour)
    interval = datetime.timedelta(hours=delta_hour)

    # get article data by time
    v = db.get_article_by_time(date, interval)
    category_num_data = db.get_article_category_num_by_time(date, interval)
    category_num_map = dict(map(lambda x : (x['category'], x['count']), category_num_data))

    # 8문장 아래인 뉴스는 거른다.
    # data = dict(map(lambda x : (x["id"], x) if len(x['article_context'].split(". ")) > 8, v))
    data = dict()
    for x in v:
        if len(x['article_context'].split(". ")) > 8:
            data[x['id']] = x
            
    record_num = len(data.keys())

    # clustering
    cluster = DBSCAN_news(list(data.values()))     
    cluster_num = cluster.info()['cluster_num'] + cluster.info()['noise_num']

    # minimize too big cluster
    cluster_id_set = []

    while True:
        cluster.dataframe["id"] = list(cluster.dataframe.index)
        total_data = cluster.dataframe.to_dict("index")

        # edit eps to minimize cluster
        eps -= 0.04
        if eps < 0.02:
            break

        flag = True

        for cid in range(cluster_num):
            tmp = cluster.dataframe[cluster.dataframe.clusters == cid]
            # over max cluster size
            if len(tmp) > max_cnt:
                flag = False
                tmp_dict = tmp.to_dict(orient = 'records')

                tmp_cluster = DBSCAN_news(tmp_dict, eps=eps, already_vectorized=True)

                # cluster id 가 겹치지 않게 설정
                tmp_cluster.dataframe["clusters"] += cluster_num*(cid+1)

                tmp_data = tmp_cluster.dataframe.to_dict("index")

                for k, v in tmp_data.items():
                    total_data[k] = v

        # relocate cluster id 
        cluster_id_set = list(set([v["clusters"] for v in total_data.values()]))

        for v in total_data.values():
            v["clusters"] = cluster_id_set.index(v["clusters"])

        cluster.dataframe = pd.DataFrame.from_dict(total_data, "index")
        cluster_num = len(cluster_id_set)

        if flag:
            break
    
    for k, v in total_data.items():
        v["id"] = k
        v["vector"] = cluster.dataframe.loc[k, "vector"]
    
    df = pd.DataFrame.from_dict(total_data, "index")
    df["cos_sim"] = None

    # TODO upload to S3
    # save vector
    df[["id", "article_headline", "vector"]].to_pickle(os.path.join(VECTOR_FILE_DIR, f"{file_date}.pkl"))

    # ranking by cos sim group_by cluster id
    for cid in range(len(cluster_id_set)):
        tmp = df[df.clusters == cid]
        mean = sum(list(tmp["vector"]))/len(list(tmp["vector"]))

        rank = []
        for idx in tmp.index:
            simularity = cos_sim(df.loc[idx, "vector"], mean)
            df.loc[idx, "cos_sim"] = simularity
            rank.append((idx, simularity))

        rank.sort(key=lambda x : x[1], reverse=True)

        ranking = 1
        for idx, v in rank:
            df.loc[idx, "rank"] = ranking
            df.loc[idx, "score"] = len(rank)/record_num
            # category_num_map['category']
            ranking += 1

    # set cluster id not to overlap each other
    try:
        pivot_id = db.get_max_clusterid()["max"] + 1
    except:
        pivot_id = 1
    df["clusters"] += pivot_id
    
    # form data to update db
    total_data = []
    service_data = []
    no_service_data = []

    for i in df.index:
        if df["rank"][i] == 1:
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            data = (int(df["clusters"][i]), "true", i)
            sdata = (i, df["created_datetime"][i].strftime('%Y-%m-%d %H:%M:%S'), now, df["article_context"][i], df["article_url"][i], df["category"][i], df["article_headline"][i], df["image_url"][i], df["score"][i], now, df["score"][i])
            service_data.append(sdata)
        else:
            ndata = (i, )
            no_service_data.append(ndata)
            data = (int(df["clusters"][i]), "false", i)

        total_data.append(data)

    # update
    while True:
        try:
            db.update_cid_representative(total_data)
            break
        except Exception as e:
            log("update_cluster_error.txt", e)
            log("update_cluster_error.txt", "")
            timelib.sleep(1)
            db = DBController()

    while True:
        try:
            dbs.upsert_news(service_data)
            break
        except Exception as e:
            log("upsert_repr_news_error.txt", e)
            log("upsert_repr_news_error.txt", "")
            timelib.sleep(1)
            dbs = ServiceDBController()
    
    while True:
        try:
            dbs.remove_news_by_id(no_service_data)
            break
        except Exception as e:
            log("remove_service_error.txt", e)
            log("remove_service_error.txt", "")
            timelib.sleep(1)
            dbs = ServiceDBController()
    
    return len(cluster_id_set), record_num


if __name__ == "__main__":

    try:
        os.system(f"mkdir {VECTOR_FILE_DIR}")
    except:
        pass

    start_date = '2021-09-12 00:00:00'
    end_date = '2021-09-13 00:00:00'
    
    date = datetime.datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
    delta = datetime.timedelta(hours=24)
    tmp = date.strftime('%Y-%m-%d %H:%M:%S')

    while True:
        
        c_n, d_n = clustering(tmp, 24)
        print(f"{tmp} clustering finish | cluster_num : {c_n}, data_num : {d_n}\r")
        tmp = (date + delta).strftime('%Y-%m-%d %H:%M:%S')


        date += delta

        if tmp == end_date:
            break
