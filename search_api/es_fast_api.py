import os
import elasticsearch
import redis
import datetime
import json
from fastapi import FastAPI
from pydantic import BaseModel

from model import *
from kafka_module import Producer
from database.controller import ServiceDBController


app = FastAPI()
es = elasticsearch.Elasticsearch("localhost:9200")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


@app.get("/")
async def root():
    return {"message": "OK"}


@app.post("/search/recent")
def search(query: Query):
    
    dbs = ServiceDBController()

    query = query.dict()

    body = {
	"from": (query.get("page") - 1)*query.get("size"),
	"size": query.get("size"),
	"query": {
		"bool": {
                	"filter": [
				{
					"match_phrase": {
						"title": query.get("query")
					}
				},
				{
					"match_phrase": {
						"summary": query.get("query")
					}
				}
			]
		}
	},
	"sort": [
		{"date": "desc"}
        ]}

    while True:

        response = es.search(index='newsboy_news', body=body)
    
        id_list = list(map(lambda x : x['_id'], response['hits']['hits']))
        if len(id_list) == 0:
            break

        try:
            exist_id_list = dbs.check_already_serviced_data_by_news_id(id_list)
        except Exception as e:
            print(e)
            dbs = ServiceDBController()
            exist_id_list = dbs.check_already_serviced_data_by_news_id(id_list)

        exist_id_list = list(map(lambda x : str(x.get('id')), exist_id_list))
    
        if len(id_list) == len(exist_id_list):
            break
        else:
            for id in exist_id_list:
                id_list.remove(id)
            query = {"query": {"terms": {"_id": id_list}}}

            try:
                res = es.delete_by_query(index="newsboy_news", body=query)
            except:
                pass

    return response


@app.post("/kafka/log/UserNewsAttention")
def kafkaLogWrapperAPI(log: UserNewsAttention):

    log_dict= log.dict()

    # producer = Producer(config_file=f"{BASE_DIR}/kafka_config.yaml")

#    producer.send_to_topic(topic="user_news_attention", key=log_dict["news_id"], value=log_dict)

    # producer.send_to_topic(topic="test_topic", key=log_dict["news_id"], value=log_dict)

    rd = redis.StrictRedis(host='localhost', port=6379, db=0)

    log_list = json.loads(rd.get("UserNewsAttention_list"))
    keyword_list = json.loads(rd.get("AttentionKeyword_list"))

    if log_list == None:
        log_list = []
    if keyword_list == None:
        keyword_list = []
    
    timestamp = log_dict["timestamp"]
    for keyword in log_dict["keywords"]:
        tmp = {
                "keyword": keyword,
                "timestamp": timestamp
        }
        # json_dump_tmp = json.dumps(tmp, ensure_ascii=False).encode('utf-8')
        keyword_list.append(tmp)

    # json_log_dict = json.dumps(log_dict, ensure_ascii=False).encode('utf-8')
    log_list.append(log_dict)

    json_log_list = json.dumps(log_list, ensure_ascii=False).encode('utf-8')
    json_keyword_list = json.dumps(keyword_list, ensure_ascii=False).encode('utf-8')

    rd.set("UserNewsAttention_list", json_log_list)
    rd.set("AttentionKeyword_list", json_keyword_list)

    return log_dict


@app.post("/realtime/keywords_aggregate")
def aggregate_keywords(newsKeyword: NewsKeywords):
    news_dict = newsKeyword.dict()
    
    rd = redis.StrictRedis(host='localhost', port=6379, db=0)

    keyword_list = json.loads(rd.get("NewsKeyword_list"))

    if keyword_list == None:
        keyword_list = []

    timestamp = news_dict["timestamp"]
    for keyword in news_dict["keywords"]:
        tmp = {
                "keyword": keyword,
                "timestamp": timestamp
        }
        keyword_list.append(tmp)

    json_keyword_list = json.dumps(keyword_list, ensure_ascii=False).encode('utf-8')

    rd.set("NewsKeyword_list", json_keyword_list)

    return news_dict


@app.get("/realtime/top_keywords")
def top_keywords():

    rd = redis.StrictRedis(host='localhost', port=6379, db=0)
    
    top_keywords = json.loads(rd.get("TopKeywords"))
    top_keywords = dict(map(lambda i: (i + 1, top_keywords[i][0]), range(len(top_keywords))))

    news_top_keywords = json.loads(rd.get("News_TopKeywords"))
    news_top_keywords = dict(map(lambda i: (i + 1, news_top_keywords[i][0]), range(len(news_top_keywords))))

    return news_top_keywords


