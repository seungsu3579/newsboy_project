import redis
import json
import time
import datetime

TIME_INTERVAL = datetime.timedelta(minutes=10)

rd = redis.StrictRedis(host='localhost', port=6379, db=0)

if __name__ == "__main__":

    while True:

        now = datetime.datetime.now().replace(microsecond=0)

        keyword_list = rd.get("AttentionKeyword_list")
        keyword_list = json.loads(keyword_list)

        # filter and aggregate by keyword with time-window
        keyword_dict = dict()
        for idx in range(len(keyword_list)-1, -1, -1):
            ts = datetime.datetime.strptime(keyword_list[idx]["timestamp"], '%Y-%m-%d %H:%M:%S').replace(microsecond=0)

            if now - ts < TIME_INTERVAL:
                keyword = keyword_list[idx]["keyword"]
                keyword_dict[keyword] = keyword_dict.get(keyword, 0) + 1
            else:
                keyword_list.pop(idx)
        
        json_keyword_list = json.dumps(keyword_list, ensure_ascii=False).encode('utf-8')
        rd.set("AttentionKeyword_list", json_keyword_list)
        
        # find top 10 keywords
        keyword_count = list(map(lambda x: (x, keyword_dict[x]), keyword_dict.keys()))
        keyword_count = sorted(keyword_count, key=lambda x : -x[1])[:10]
        
        json_keyword_count = json.dumps(keyword_count, ensure_ascii=False).encode('utf-8')
        rd.set("TopKeywords", json_keyword_count)
        time.sleep(5)
