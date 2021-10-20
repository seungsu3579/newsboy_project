import datetime
from database.controller import ServiceDBController
from database.controller import DBController
from tqdm import tqdm
from elasticsearch import Elasticsearch
from tqdm import tqdm

es = Elasticsearch('http://52.78.20.201:9200')

if __name__ == "__main__":
    
    dbs = ServiceDBController()
    db = DBController()

    start_date = '2020-01-01 00:00:00'
    end_date = '2020-02-01 00:00:00'

    # data = db.get_representative_article(start_date, end_date)

    ###################################################################

    data = dbs.get_news_to_summarize((start_date, end_date))

    for d in tqdm(data):
        id = d.get("id")

        doc = {
            "title" : d.get("headline"),
            "summary" : d.get("article"),
            "date" : d.get("created_date_time"),
            "image_url" : d.get("image_url"),
            "article_url" : d.get("article_url")
        }
        
        es.create(index="newsboy_test", id=id, body=doc)
    