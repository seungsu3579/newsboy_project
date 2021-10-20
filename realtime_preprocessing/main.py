import time
import datetime
from realtime import main_process
from utils import *
from pprint import pprint



EXECUTION_CHECK_TIME_INTERVAL = 2
THREAD_NUM = 4

NEWS_REWORKERS = [NewsReworker() for i in range(THREAD_NUM)]
VECTORIZERS = []

if __name__ == "__main__":

    today = datetime.datetime.strftime(datetime.datetime.now(datetime.timezone.utc), '%Y-%m-%d %H:%M:%S')[:10]

    now = datetime.datetime.now() - datetime.timedelta(hours=7) - datetime.timedelta(minutes=41)
    last_execution_time = now.replace(hour=0, minute=0, second=0, microsecond=0)

    while True:
        now = datetime.datetime.now() - datetime.timedelta(hours=7) - datetime.timedelta(minutes=41)
        if now - last_execution_time > datetime.timedelta(minutes=EXECUTION_CHECK_TIME_INTERVAL):
            
            time_interval = (now - last_execution_time).seconds
            try:
                execution_status = main_process(THREAD_NUM, time_interval, NEWS_REWORKERS)
            except:
                continue
            pprint(execution_status)
            last_execution_time = execution_status.get("timestamp")
        
        else:
            time.sleep(30)
