import datetime
import threading
import unicodedata as unicode
from tqdm import tqdm

from pororo import Pororo
from elasticsearch import Elasticsearch

from utils import *
from clustering.dbscan import DBSCAN_news
from database.controller import DBController, ServiceDBController




# settings

# time setting
date ='2020-01-10 13:05:00'
FILE_DATE_NAME = date[:10]
date = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
delta_hour = 1
interval = datetime.timedelta(hours=delta_hour)

# date = datetime.datetime.now()
# FILE_DATE_NAME = datetime.datetime.strftime(date, '%Y-%m-%d %H:%M:%S')[:10]
# interval = datetime.timedelta(minutes=NEWS_TIME_INTERVAL)

NEWS_TIME_INTERVAL = 5
TMP_VECTORIZED_NEWS = f"{FILE_DATE_NAME}_vectorized_news.pkl"


def summarize_keyword(text_list : list, keyword_news_list : list, news_summary_list : list, lock, keyword_lock):
    
    summ = Pororo(task="summarization", model="extractive", lang="ko")
    ner = Pororo(task="ner", lang="ko")
    pos = Pororo(task="pos", lang="ko")
    es = Elasticsearch("http://search.newsboycorp.com:9200")
    tmp_dbs = ServiceDBController()
    
    error_count = 0
    
    for text in tqdm(text_list):

        
        ##### summarize #####
        try:
            summarized_text = summ(text.get("article_context"))
        except:
            article = text.get("article_context")
            summarized_text = ". ".join(article.split(". ")[:3])
            summarized_text += "...(후략)"

            error_count += 1
        
        ##### elastic search add #####
        id = text.get("id")
        doc = {'title' : text.get('article_headline'), 'summary' : summarized_text, 'article_url' : text.get('article_url'), 'image_url' : text.get('image_url'), "date": text.get("created_datetime")}

        lock.acquire()
        # TODO
#         try:
#             es.create(index="newsboy_news", id=id, body=doc)
#         except:
#             pass
        print(f"ES INSERT : {doc}")
        lock.release()

        ##### extract keyword #####
        keywords = set()
        if len(summarized_text) > 512:
            summarized_text_keyword = summarized_text[:512]
        else:
            summarized_text_keyword = summarized_text
        
        try:
            ner_analysis = ner(summarized_text_keyword)
        except:
            ner_analysis = []
            # continue

        flag = True
        for word, tp in ner_analysis:
            if tp not in ['DATE', 'QUANTITY', 'O', 'COUNTRY', 'LOCATION', 'OCCUPATION', 'TIME', 'CITY', 'CIVILIZATION', 'TERM', 'ANIMAL'] and len(word) > 1:
                if word.isalnum() and (word not in STOPWORDS):
                    # keywords.add((word, tp))
                    for c in word:
                        if 'HANGUL' not in unicode.name(c) and 'LATIN' not in unicode.name(c) and 'DIGIT' not in unicode.name(c):
                            flag = False
                            break
                    if flag:
                        keywords.add((word, tp))
                        flag = True
        
        for wordlist, tp in [(pos(word), tp) for word, tp in list(keywords)]: 
            if len(wordlist) == 1:
                if len(wordlist[0]) == 0 or wordlist[0][1] != 'NNP':
                    keywords.remove((wordlist[0][0], tp))
        


        # upsert keyword on keyword table    
        upsert_keyword_list = []
        for keyword, tp in keywords:
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            upsert_keyword_list.append((now, now, keyword, tp, now, keyword,))
        

        # TODO
#         keyword_lock.acquire()
#         tmp_dbs.upsert_keyword(upsert_keyword_list)
#         keyword_lock.release()
        print(f"KEYWORD UPSERT : {upsert_keyword_list}")

        # save relation of keyword and article
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        keyword = list(map(lambda x : "\'" + x[0] + "\'", keywords))
        if len(keyword) != 0:
            keyword_news_relation_list = [(now, now, row['keyword_id'], text.get("id"), now) for row in tmp_dbs.get_keyword_ids(", ".join(keyword))] 
        else:
            keyword_news_relation_list = []
        # tmp_dbs.insert_keyword_news_relation(keyword_news_relation_list)

        # lock
        lock.acquire()
        keyword_news_list += keyword_news_relation_list
        news_summary_list.append(
            (text.get("id"), text.get("created_datetime").strftime('%Y-%m-%d %H:%M:%S'), now, summarized_text, text.get("article_url"), 
             text.get("category"), text.get("article_headline"), text.get("image_url"), text.get("score"), now, text.get("score"), )
        )
        lock.release()




if __name__ == "__main__":
    
    # get recent news to vectorize
    db = DBController()
    dbs = ServiceDBController()


    # get news data recent 5min data
    recent_news_tmp = db.get_article_by_realtime(date, interval)
    recent_news_data = dict()
    for news in recent_news_tmp:
        if len(news['article_context'].split('. ')) > 8:
            recent_news_data[news['id']] = dict(news)

    
    # get previous news data already vectorized
    try:
        prev_news_dict = DataLoader.load_pickle_as_object(TMP_VECTORIZED_NEWS)
    except FileNotFoundError:
        prev_news_dict = dict()
    

    # filtering duplicate news and time 
    new_news_list = []
    # duplicate_list = []
    cnt = 1
    for k, v in recent_news_data.items():
        if prev_news_dict.get(k) == None:
            new_news_list.append(v)
        # else:
        #     duplicate_list.append(v)


    # vectorize recent news
    recent_news_dict = Vectorizer.set_df_thread_pool(new_news_list, 8)


    # merge previous data and recent data
    all_data = prev_news_dict
    all_data.update(recent_news_dict)
    record_num = len(all_data.keys())


    # save vectorized data
    DataLoader.save_object_as_pickle(all_data, TMP_VECTORIZED_NEWS)


    # first clustering
    all_data = prev_news_dict
    all_data.update(recent_news_dict)
    cluster = DBSCAN_news(list(all_data.values()), already_vectorized=True)
    cluster_num = cluster.info()['cluster_num'] + cluster.info()['noise_num']

    # minimize too big cluster
    cluster_id_set = []
    max_cnt = 300
    eps = 0.15
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


    # form data to update db & parsing record(serviced data, not serviced data)
    total_data = []
    service_data = dict()
    no_service_data = dict()

    for i in df.index:
        if df["rank"][i] == 1:
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            data = (int(df["clusters"][i]), "true", i)
            sdata = (i, df["created_datetime"][i].strftime('%Y-%m-%d %H:%M:%S'), now, df["article_context"][i], df["article_url"][i], df["category"][i], df["article_headline"][i], df["image_url"][i], df["score"][i], now, df["score"][i])
            service_data[i] = sdata
        else:
            ndata = (i, )
            no_service_data[i] = ndata
            data = (int(df["clusters"][i]), "false", i)

        total_data.append(data)


    # find news that must be summarized
    check_list = list(map(lambda x: x, service_data.keys()))
    check_list = dbs.check_already_serviced_data_by_news_id(check_list)
    check_list = list(map(lambda x : x.get("id"), check_list))
    summ_data = [all_data[k] for k in check_list]

    
    # start summarizing news and extracting keyword
    idx = 0
    thread_num = 8
    parallel_data_list = [[]for i in range(thread_num)]
    while len(summ_data) != 0:
        parallel_data_list[idx%thread_num].append(summ_data.pop())
        idx += 1
    
    STOPWORDS = ["한국일보", "매일경제", "일다", "이코노미스트", "조선일보", "국민일보", "MBN", "시사IN", "이데일리", "한경비즈니스", "서울신문", "월간 산", "더팩트", "헤럴드경제", "한국경제", "블로터", "SBS Biz", "뉴스1", "헬스조선", "디지털데일리", "YTN", "중앙SUNDAY", "코메디닷컴", "디지털타임스", "한겨레21", "노컷뉴스", "JTBC", "파이낸셜뉴스", "서울경제", "중앙일보", "세계일보", "매일신문", "TV조선", "동아일보", "기자협회보", "오마이뉴스", "한겨레", "매경이코노미", "경향신문", "한국경제TV", "여성신문", "강원일보", "코리아중앙데일리", "연합뉴스", "연합뉴스TV", "문화일보", "시사저널", "비즈니스워치", "신동아", "조세일보", "머니투데이", "레이디경향", "주간경향", "미디어오늘", "채널A", "부산일보", "프레시안", "동아사이언스", "코리아헤럴드", "ZDNet Korea", "SBS", "아이뉴스24", "뉴시스", "뉴스타파", "머니S", "데일리안", "전자신문", "조선비즈", "아시아경제", "MBC", "KBS", "주간조선", "주간동아"]
    keyword_news_list = []
    news_summary_list = []
    lock = threading.Lock();
    keyword_lock = threading.Lock();

    thread_list = []
    print("thread starting...")
    for tmp_data in parallel_data_list:

        tmp_thread = threading.Thread(target=summarize_keyword, args=(tmp_data, keyword_news_list, news_summary_list, lock, keyword_lock, ), daemon=True)
        thread_list.append(tmp_thread)
        tmp_thread.start()

    for th in thread_list:
        th.join()


    # form news summary data for update service db
    for v in news_summary_list:
        tmp = list(service_data[v[0]])
        tmp[3] = v[3]
        service_data[v[0]] = tuple(tmp)

    
    # update db
    # TODO
    # db.update_cid_representative(total_data)
    print(total_data[:10])

    # dbs.upsert_news(list(service_data.values()))
    print(list(service_data.values())[:10])

    # dbs.insert_keyword_news_relation(keyword_news_list)
    print(keyword_news_list[:10])

    # dbs.remove_news_by_id(list(no_service_data.values()))
    print(list(no_service_data.values())[:10])

    # dbs.remove_keyword_news_mapping_by_newsid(list(service_data.values()))
    print(list(service_data.values())[:10])