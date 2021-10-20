#-*- coding: utf-8 -*-
import psycopg2
import psycopg2.extras
from .config import CONFIG as CRAWLING_CONFIG
from .config_service import CONFIG as SERVICE_CONFIG

class ServiceDBController:
    def __init__(self):
        self.conn = psycopg2.connect(
            user=SERVICE_CONFIG["RDS_configure"]["postgresql_user"],
            password=SERVICE_CONFIG["RDS_configure"]["postgresql_password"],
            host=SERVICE_CONFIG["RDS_configure"]["postgresql_host"],
            port=SERVICE_CONFIG["RDS_configure"]["postgresql_port"],
            database=SERVICE_CONFIG["RDS_configure"]["postgresql_database"],
        )
        # 딕셔너리 형태로 반환
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def __del__(self):
        self.conn.close()

    def get_total_num_to_summarize(self, data):
        try:
            sql = "SELECT count(id) FROM news WHERE is_summary = false AND created_date_time between %s and %s;"
            self.cur.execute(sql, data)
            array = self.cur.fetchall()
            return array
        except Exception as e:
            print(e)
            raise e

    def remove_keyword_news_mapping_by_newsid(self, data : list):
        try:
            sql = "DELETE FROM news_keyword WHERE news_id = %s"
            self.cur.executemany(sql, data)
            self.conn.commit()
        except Exception as e:
            print(e)
            raise e
    
    def check_already_serviced_data_by_news_id(self, data: list):
        try:
            sql = f"SELECT news.id FROM news WHERE id IN ({str(data).strip('[]')})"
            self.cur.execute(sql)
            array = self.cur.fetchall()
            return array
        except Exception as e:
            print(e)
            raise e
            
    def get_news_to_summarize(self, data):
        try:
            # sql = f"SELECT id, article, image_url, headline, article_url FROM news WHERE is_summary = false AND created_date_time between %s and %s LIMIT 800"
            sql = f"SELECT id, article, image_url, headline, article_url, created_date_time FROM news WHERE is_summary = false AND created_date_time between %s and %s LIMIT 3200"
            
            self.cur.execute(sql, data)
            array = self.cur.fetchall()
            return array
        except Exception as e:
            print(e)
            raise e
    
    def get_news_to_summarize_test(self, data):
        try:
            sql = f"SELECT id, article, image_url, headline, article_url FROM news WHERE id = %s"
            
            self.cur.execute(sql, data)
            array = self.cur.fetchall()
            return array
        except Exception as e:
            print(e)
            raise e

    def get_keyword_ids(self, data: str):
        try:
            sql = f"SELECT keyword_id FROM keyword WHERE keyword.word IN ({data});"
            self.cur.execute(sql)
            array = self.cur.fetchall()
            return array
        except Exception as e:
            print(e)
            raise e
    
    def insert_keyword_news_relation(self, data: list):
        try:
            sql = """
            INSERT INTO news_keyword (created_date_time, last_modified_date_time, keyword_id, news_id) 
            VALUES (%s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT news_keyword_mapping_unique
            DO UPDATE
            SET last_modified_date_time = %s            
            """

            self.cur.executemany(sql, data)
            self.conn.commit()
        except Exception as e:
            print(e)
            raise e
    
    def update_news_summary(self, data : list):
        try:
            sql = """
            UPDATE news SET (article, is_summary) = (%s, true) WHERE id = %s
            """

            self.cur.executemany(sql, data)
            self.conn.commit()
        except Exception as e:
            print(e)
            raise e

    def upsert_keyword(self, data : list):
        try:
            sql = """
            INSERT INTO keyword (created_date_time, last_modified_date_time, word, type) 
            VALUES (%s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT keyword_unique
            DO UPDATE
            SET (last_modified_date_time, word) = (%s, %s)
            """

            self.cur.executemany(sql, data)
            self.conn.commit()
        except Exception as e:
            print(e)
            raise e
    
    def remove_news_by_id(self, data : list):
        try:
            sql = "DELETE FROM news WHERE id = %s"
            self.cur.executemany(sql, data)
            self.conn.commit()
        except Exception as e:
            print(e)
            raise e

    def upsert_news(self, data : list):
        try:
            sql = """
            INSERT INTO news (id, created_date_time, last_modified_date_time, article, article_url, category, headline, image_url, score) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT news_pkey
            DO UPDATE
            SET (last_modified_date_time, score) = (%s, %s)
            """

            self.cur.executemany(sql, data)
            self.conn.commit()
        except Exception as e:
            print(e)
            raise e
    
    def update_article_context(self, data:list):
        try:
            sql = """
            UPDATE news SET (article, is_summary) = (%s, false) WHERE id = %s
            """
            self.cur.executemany(sql, data)
            self.conn.commit()
        except Exception as e:
            print(e)
            raise e




class DBController:
    def __init__(self):
        self.conn = psycopg2.connect(
            user=CRAWLING_CONFIG["RDS_configure"]["postgresql_user"],
            password=CRAWLING_CONFIG["RDS_configure"]["postgresql_password"],
            host=CRAWLING_CONFIG["RDS_configure"]["postgresql_host"],
            port=CRAWLING_CONFIG["RDS_configure"]["postgresql_port"],
            database=CRAWLING_CONFIG["RDS_configure"]["postgresql_database"],
        )
        # 딕셔너리 형태로 반환
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def __del__(self):
        self.conn.close()

    
    def get_representative_article(self, timestamp, time_interval):
        try:
            sql = "SELECT id, article_context FROM news WHERE created_datetime between %s and %s AND representative IS true"
            self.cur.execute(sql, (timestamp, time_interval))
            array = self.cur.fetchall()
            return array
        except Exception as e:
            print(e)
            raise e
    
    def get_max_clusterid(self, ):
        try:
            sql = "SELECT max(cluster_id) FROM news;"
            self.cur.execute(sql)
            array = self.cur.fetchone()
            return array
        except Exception as e:
            raise e

    def get_article_category_num_by_time(self, timestamp, time_interval):
        """k
        input :
            timestamp : 기준 시간
            time_interval: 기준 시간으로부터 유효 시간
        return : 
            해당 시간에 맞는 keyword list
        """
        try:
            sql = "SELECT category, count(*) FROM news WHERE created_datetime between %s and %s AND article_context is not NULL GROUP BY category"
            self.cur.execute(sql, (timestamp.strftime('%Y-%m-%d %H:%M:%S'), (timestamp+time_interval).strftime('%Y-%m-%d %H:%M:%S'), ))
            array = self.cur.fetchall()
            return array
        except Exception as e:
            print(e)
            raise e

    def get_article_by_time(self, timestamp, time_interval):
        """k
        input :
            timestamp : 기준 시간
            time_interval: 기준 시간으로부터 유효 시간
        return : 
            해당 시간에 맞는 keyword list
        """
        try:
            sql = "SELECT id, article_headline, keywords, cluster_id, created_datetime, article_context, article_url, category, image_url FROM news WHERE created_datetime between %s and %s AND article_context is not NULL"
            self.cur.execute(sql, (timestamp.strftime('%Y-%m-%d %H:%M:%S'), (timestamp+time_interval).strftime('%Y-%m-%d %H:%M:%S'), ))
            array = self.cur.fetchall()
            return array
        except Exception as e:
            print(e)
            raise e

    def get_article_by_realtime(self, timestamp, time_interval):
        """k
        input :
            timestamp : 기준 시간
            time_interval: 기준 시간으로부터 유효 시간
        return : 
            해당 시간에 맞는 keyword list
        """
        try:
            sql = "SELECT id, article_headline, keywords, cluster_id, created_datetime, article_context, article_url, category, image_url FROM news WHERE created_datetime between %s and %s AND article_context is not NULL"
            print((timestamp-time_interval).strftime('%Y-%m-%d %H:%M:%S'), "~", timestamp.strftime('%Y-%m-%d %H:%M:%S'))
            self.cur.execute(sql, ((timestamp-time_interval).strftime('%Y-%m-%d %H:%M:%S'), timestamp.strftime('%Y-%m-%d %H:%M:%S'), ))
            array = self.cur.fetchall()
            return array
        except Exception as e:
            print(e)
            raise e

    def get_article_context(self, category: str, date: str):
        try:
            sql = (
                f"SELECT id, article_context FROM news WHERE category = '{category}' "
                f"AND date(created_datetime) = '{date}' AND article_context IS NOT NULL"
            )
            self.cur.execute(sql)
            array = self.cur.fetchall()
            return array
        except Exception as e:
            raise e

    def update_keywords(self, data: list):
        try:
            sql = "UPDATE news SET keywords = %s where id = %s"
            self.cur.executemany(sql, data)
            self.conn.commit()
        except Exception as e:
            print(e)
            raise e
    
    def update_cid_representative(self, data: list):
        try:
            sql = "UPDATE news SET cluster_id = %s, representative = %s where id = %s"
            self.cur.executemany(sql, data)
            self.conn.commit()
        except Exception as e:
            print(e)
            raise e

    def get_keywords_by_time(self, timestamp, time_interval):
        """
        input :
            timestamp : 기준 시간
            time_interval: 기준 시간으로부터 유효 시간
        return : 
            해당 시간에 맞는 keyword list
        """

        try:
            # sql = "SELECT id, article_headline, keywords, cluster_id FROM news WHERE created_datetime between %s and %s AND keywords is not null;"
            sql = "SELECT id, article_headline, keywords, cluster_id, created_datetime, article_context, article_url, category, image_url FROM news WHERE created_datetime between %s and %s AND keywords is not null;"
            self.cur.execute(sql, ((timestamp-time_interval).strftime('%Y-%m-%d %H:%M:%S'), timestamp.strftime('%Y-%m-%d %H:%M:%S'), ))
            array = self.cur.fetchall()
            return array
        except Exception as e:
            print(e)
            raise e

    def get_keywords_by_clusterid(self, cluster_ids:list):
        """
        input :
            cluster_ids : 클러스터 id 리스트
        return :
            cluster_id 리스트에 해당되는 모든 news의 keyword list
        """
        try:
            # sql = f"SELECT id, article_headline, keywords, cluster_id FROM news WHERE cluster_id IN ({str(cluster_ids).strip('[]')}) AND keywords is not null"
            sql = f"SELECT id, article_headline, keywords, cluster_id, created_datetime, article_context, article_url, category, image_url FROM news WHERE cluster_id IN ({str(cluster_ids).strip('[]')}) AND keywords is not null"
            self.cur.execute(sql)
            array = self.cur.fetchall()
            return array
        except Exception as e:
            print(e)
            raise e
        

    def get_news_by_time(self, timestamp, time_interval):
        """
        input :
            timestamp : 기준 시간
            time_interval: 기준 시간으로부터 유효 시간
        return : 
            해당 시간에 맞는 keyword list
        """
        try:
            sql = "SELECT id, article_headline, keywords FROM news WHERE created_datetime between %s and %s AND keywords is not null;"
            self.cur.execute(
                sql,
                (
                    (timestamp - time_interval).strftime("%Y-%m-%d %H:%M:%S"),
                    timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
            array = self.cur.fetchall()
            return array
        except Exception as e:
            print(e)
            raise e

    def get_all_by_time(self, timestamp, time_interval):
        """
        input :
            timestamp : 기준 시간
            time_interval: 기준 시간으로부터 유효 시간
        return : 
            해당 시간에 맞는 keyword list
        """
        try:
            sql = "SELECT id, category, article_headline, article_context, article_url, image_url FROM news WHERE created_datetime between %s and %s AND keywords IS NOT NULL;"
            self.cur.execute(sql, ((timestamp-time_interval).strftime('%Y-%m-%d %H:%M:%S'), timestamp.strftime('%Y-%m-%d %H:%M:%S'), ))
            array = self.cur.fetchall()
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
