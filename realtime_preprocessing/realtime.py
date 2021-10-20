import time
import datetime
import threading
import unicodedata as unicode
from tqdm import tqdm

from pororo import Pororo
from elasticsearch import Elasticsearch

from error import *
from utils import *
from clustering.dbscan import DBSCAN_news
from database.controller import DBController, ServiceDBController



def main_process(thread_num : int, time_interval : int, news_reworker_list: list):
    

    status = dict()

    ##############
    ## settings ##
    ##############
    NOW = datetime.datetime.now() - datetime.timedelta(hours=7) - datetime.timedelta(minutes=41)
    NEWS_TIME_INTERVAL = (time_interval + 300) // 60
    THREAD_NUM = thread_num
    max_cnt = 300
    eps = 0.15
    FILE_DATE_NAME = datetime.datetime.strftime(NOW, '%Y-%m-%d %H:%M:%S')[:10]
    TMP_VECTORIZED_NEWS = f"{FILE_DATE_NAME}_vectorized_news.pkl"
    db = DBController()
    dbs = ServiceDBController()
    status["timestamp"] = NOW

    print("preprocessing...")
    preprocessing_start = time.time()
    ####################################
    ## get news data recent 5min data ##
    ####################################
    try:
        recent_news_data = DataLoader.load_recent_news_data(NOW, NEWS_TIME_INTERVAL)
    except NoRecentNewsDataError:
        status["status"] = "[No Recent News Data Error]"
        return status


    ###############################################
    ## get previous news data already vectorized ##
    ###############################################
    try:
        prev_news_dict = DataLoader.load_pickle_as_object(TMP_VECTORIZED_NEWS)
    except FileNotFoundError:
        prev_news_dict = dict()


    #######################################
    ## filtering duplicate news and time ##
    #######################################
    new_news_list = []
    # duplicate_list = []
    cnt = 1
    for k, v in recent_news_data.items():
        if prev_news_dict.get(k) == None:
            new_news_list.append(v)
        # else:
        #     duplicate_list.append(v)
    status["preprocessing_time"] = time.time() - preprocessing_start


    print("vectorizing...")
    vectorizing_start = time.time()
    ###########################
    ## vectorize recent news ##
    ###########################
    recent_news_dict = Vectorizer.set_df_thread_pool(new_news_list, THREAD_NUM)
    status["vectorizing_time"] = time.time() - vectorizing_start


    #########################################
    ## merge previous data and recent data ##
    #########################################
    all_data = prev_news_dict
    all_data.update(recent_news_dict)
    record_num = len(all_data.keys())


    ##########################
    ## save vectorized data ##
    ##########################
    DataLoader.save_object_as_pickle(all_data, TMP_VECTORIZED_NEWS)



    print("clustering...")
    clustering_start = time.time()
    ######################
    ## first clustering ##
    ######################
    cluster = DBSCAN_news(list(all_data.values()), already_vectorized=True)
    cluster_num = cluster.info()['cluster_num'] + cluster.info()['noise_num']

    ##############################
    ## minimize too big cluster ##
    ##############################
    cluster_id_set = []
    while True:
        cluster.dataframe.loc[:, "id"] = list(cluster.dataframe.index)
        total_data = cluster.dataframe.to_dict("index")

        ## edit eps to minimize cluster
        eps -= 0.04
        if eps < 0.02:
            break

        flag = True

        for cid in range(cluster_num):
            tmp = cluster.dataframe[cluster.dataframe.clusters == cid]

            ## when over max cluster size
            if len(tmp) > max_cnt:
                flag = False
                tmp_dict = tmp.to_dict(orient = 'records')

                tmp_cluster = DBSCAN_news(tmp_dict, eps=eps, already_vectorized=True)

                ## set cluster id not to overlap
                tmp_cluster.dataframe.loc[:,"clusters"] += cluster_num*(cid+1)

                tmp_data = tmp_cluster.dataframe.to_dict("index")

                for k, v in tmp_data.items():
                    total_data[k] = v

        ## relocate cluster id by order
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


    ############################################
    ## ranking by cos sim group_by cluster id ##
    ############################################
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


    ##############################################
    ## set cluster id not to overlap each other ##
    ##############################################
    try:
        pivot_id = db.get_max_clusterid()["max"] + 1
    except:
        pivot_id = 1
    df["clusters"] += pivot_id
    status["clustering_time"] = time.time() - clustering_start




    ###############################################################################
    ## form data to update db & parsing record(serviced data, not serviced data) ##
    ###############################################################################
    total_data = []
    service_data = dict()
    no_service_data = dict()

    for i in df.index:
        if df["rank"][i] == 1:
            now = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            data = (int(df["clusters"][i]), "true", i)
            sdata = (i, df["created_datetime"][i].strftime('%Y-%m-%d %H:%M:%S'), now, df["article_context"][i], df["article_url"][i], df["category"][i], df["article_headline"][i], df["image_url"][i], df["score"][i], now, df["score"][i])
            service_data[i] = sdata
        else:
            ndata = (i, )
            no_service_data[i] = ndata
            data = (int(df["clusters"][i]), "false", i)

        total_data.append(data)


    print("summarizing...")
    summarizing_start = time.time()
    #######################################
    ## find news that must be summarized ##
    #######################################
    check_list = list(map(lambda x: x, service_data.keys()))
    serviced_list = dbs.check_already_serviced_data_by_news_id(check_list)
    serviced_list = list(map(lambda x : x.get("id"), serviced_list))
    not_serviced_list = list(filter(lambda x: x not in serviced_list, service_data.keys()))
    not_serviced_list = list(map(lambda x: all_data[x], not_serviced_list))

    ###################################################
    ## start summarizing news and extracting keyword ##
    ###################################################
    idx = 0
    parallel_data_list = [[]for i in range(THREAD_NUM)]
    while len(not_serviced_list) != 0:
        parallel_data_list[idx%THREAD_NUM].append(not_serviced_list.pop())
        idx += 1

    keyword_news_list = []
    news_summary_list = []
    lock = threading.Lock();
    keyword_lock = threading.Lock();

    thread_list = []
    print("starting summarizing news and extracting keyword...")
    for th, tmp_data in enumerate(parallel_data_list):
        # TODO DEBUGGING
        tmp_thread = threading.Thread(target=news_reworker_list[th].summarize_keyword, args=(tmp_data, keyword_news_list, news_summary_list, lock, keyword_lock, ), daemon=True)
        thread_list.append(tmp_thread)
        tmp_thread.start()

    for th in thread_list:
        th.join()
    status["summarizing_time"] = time.time() - summarizing_start


    
    ##################################################
    ## form news summary data for update service db ##
    ##################################################
    for v in news_summary_list:
        tmp = list(service_data[v[0]])
        tmp[3] = v[3]
        service_data[v[0]] = tuple(tmp)

    database_updating_start = time.time()
    ###############
    ## update db ##
    ###############
    # TODO
    tmp_start = time.time()
    db.update_cid_representative(total_data)
    # print(total_data[:3])
    status["step1_time"] = time.time() - tmp_start

    tmp_start = time.time()
    dbs.upsert_news(list(service_data.values()))
    # print(list(service_data.values())[:3])
    status["step2_time"] = time.time() - tmp_start

    tmp_start = time.time()
    dbs.insert_keyword_news_relation(keyword_news_list)
    # print(keyword_news_list[:3])
    status["step3_time"] = time.time() - tmp_start

    tmp_start = time.time()
    dbs.remove_keyword_news_mapping_by_newsid(list(no_service_data.values()))
    # print(list(service_data.values())[:3])
    status["step4_time"] = time.time() - tmp_start
    
    tmp_start = time.time()
    dbs.remove_news_by_id(list(no_service_data.values()))
    # print(list(no_service_data.values())[:3])

    es = Elasticsearch("http://search.newsboycorp.com:9200")
    delete_id_list = list(map(lambda x : str(x[0]), no_service_data.values()))
    query = {"query": {"terms": {"_id": delete_id_list}}}
    try:
        res = es.delete_by_query(index="newsboy_news", body=query)
    except:
        pass
    status["step5_time"] = time.time() - tmp_start

    status["database_updating_time"] = time.time() - database_updating_start
    status["status"] = "[OK]"

    return status