# coding: utf-8

__author__ = 'AnnieQin <annie__qin@163.com>'

import calendar
from datetime import *
import requests
import pymongo
from peewee import *
from proxy import Proxy
import logging
import threading
import multiprocessing
import os
from Queue import Queue

logging.basicConfig(filename='crawl_multithread.log', level=logging.INFO)

mongo_conn = pymongo.MongoClient('localhost', 27017)
mongo_db = mongo_conn['stackcrawler']

mysql_db = MySQLDatabase('stackoverflow', host='127.0.0.1', port=3306, user='root')

CLIENT_ID = '6570'
CLIENT_SECRET = 'HT29yaLxlCiz3Y4xbPzXAg(('
KEY = 'L38kOBC7bsGjecSfPUR)Ag(('

payload = {
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
    'key': KEY
    }


class BaseModel(Model):
    class Meta:
        database = mysql_db


def create_tables():
    mysql_db.connect()
    mysql_db.create_tables([Question], safe=True)


class Question(BaseModel):
    question_id = CharField(max_length=100, default='')
    link = CharField(max_length=1000, default='')
    title = CharField(max_length=1000, default='')

    owner_id = CharField(max_length=100, default='')

    view_count = CharField(max_length=100, default='')
    score = CharField(max_length=100, default='')

    creation_date = BigIntegerField(null=True, default=None)
    last_activity_date = BigIntegerField(null=True, default=None)

    is_answered = BooleanField(null=True, default=None)
    answer_count = CharField(max_length=100, null=True, default=None)
    accepted_answer_id = CharField(max_length=100, null=True, default=None)


def requests_get(page, fromdate=None, todate=None, headers=None, proxies=None):
    url = 'https://api.stackexchange.com/questions?site=stackoverflow'
    # payload['sort'] = 'hot'
    payload['pagesize'] = 100
    payload['page'] = page
    print threading.currentThread().getName()+'  Crawling  PAGE '+str(page)
    logging.info(threading.currentThread().getName()+'  Crawling  PAGE '+str(page))
    if fromdate:
        payload['fromdate'] = fromdate
    if todate:
        payload['todate'] = todate

    ret = None
    try:
        ret = requests.get(url=url,
                           # timeout=2,
                           params=payload,
                           headers=headers,
                           proxies=proxies)
    except:
        logging.exception('message')
        print threading.currentThread().getName()+'  Crawling EXCEPTION'

    return ret


def get_pros(start_id):
    pros = Proxy.select().where(Proxy.id >= start_id, Proxy.status != 0, Proxy.type == 'https')
    ret = [{'ip': p.ip, 'port': p.port, 'id': p.id} for p in pros]
    return ret


def mongodb_save(items, page):
    if rlock2.acquire():
        start_time = datetime.now()
        try:
            bulk = mongo_db.questiontags.initialize_unordered_bulk_op()
            for item in items:
                bulk.find({'question_id': item['question_id']}).upsert().update({'$set': {'tags': item.get('tags', '')}})
            bulk.execute()

            end_time = datetime.now()
            print threading.currentThread().getName()+'  PAGE'+str(page)+'  mongodb bulk upsert finishes, consumes: '+str(end_time-start_time)
            logging.info(threading.currentThread().getName()+'  PAGE'+str(page)+'  mongodb bulk upsert finishes, consumes: '+str(end_time-start_time))
        except:
            print threading.currentThread().getName()+'  PAGE'+str(page)+'  mongodb bulk insert exception'
            logging.exception('message')

        rlock2.release()


def mysql_save(items, page):
    if rlock3.acquire():
        start_time = datetime.now()
        try:
            with mysql_db.atomic():
                for item in items:
                    question = Question.select().where(Question.question_id == item['question_id']).exists()
                    if not question:
                        Question.create(**item)
                    # Question.get_or_create(**item)  # 慢！
            # with mysql_db.atomic():
            #     Question.insert_many(items).execute()

            end_time = datetime.now()
            print threading.currentThread().getName()+'  PAGE'+str(page)+'  mysql bulk insert finishes, consumes: '+str(end_time-start_time)
            logging.info(threading.currentThread().getName()+'  PAGE'+str(page)+'  mysql bulk insert finishes, consumes: '+str(end_time-start_time))
        except IntegrityError:
            print threading.currentThread().getName()+'  PAGE'+str(page)+'  mysql bulk insert exception'
            logging.exception('message')

        rlock3.release()


def crawling(page, pro):
        today = date.today()

        todate = None
        # todate = calendar.timegm(today.timetuple())
        fromdate = calendar.timegm((today - timedelta(days=0.5)).timetuple())

        # keys_to_remain = ['question_id', 'link', 'title', 'view_count',
        #                   'score', 'creation_date', 'last_activity_date',
        #                   'is_answered', 'answer_count', 'accepted_answer_id']
        #
        # keys_to_delete = ['tags', 'owner', 'bounty_amount',
        #                   'bounty_closes_date', 'community_owned_date',
        #                   'locked_date', 'migrated_from',
        #                   'migrated_to', 'protected_date',
        #                   'last_edit_date', 'closed_date', 'closed_reason']

        # 抓取网页
        c_start = datetime.now()

        res = requests_get(page, fromdate=fromdate, todate=todate)
        if not res or res.status_code != 200:
            res = requests_get(page, fromdate=fromdate, todate=todate,
                               proxies={'https': 'https://%s:%s' % (pro['ip'], pro['port'])})
        c_finish = datetime.now()
        print threading.currentThread().getName()+'  PAGE'+str(page)+' 抓取时间: '+str(c_finish-c_start)
        logging.info(threading.currentThread().getName()+'  PAGE'+str(page)+' 抓取时间: '+str(c_finish-c_start))

        if not res:
            return 2

        # 抓取网页成功
        if res:
            if res.status_code == 200:
                print threading.currentThread().getName()+'  Crawling SUCCESS PAGE'+str(page)
                logging.info(threading.currentThread().getName()+'  Crawling SUCCESS PAGE'+str(page))

                # 解析res,保存抓下来的数据
                items = res.json()['items']
                # question tags存入MongoDB
                mongodb_save(items, page)

                # for item in items:
                #     item['owner_id'] = item.get('owner', '').get('user_id', '')
                #
                #     for k in keys_to_remain:
                #         item[k] = item.get(k, '')
                #     for k in keys_to_delete:
                #         item.pop(k, None)
                # 数据存入MySQL
                mysql_save(items, page)

                queue.task_done()

                # 是否还有数据，若有page+1继续抓取
                has_more = res.json()['has_more']
                if has_more:
                    for _ in range(1, 8):
                        page += 1
                        queue.put(page)
                    return 1
                if not has_more:
                    return 0

            # 抓取网页未返回200
            elif res.status_code != 200:
                print threading.currentThread().getName()+'PAGE'+str(page)+'  Crawling FAILURE  RES '+str(res.status_code)

                logging.info(threading.currentThread().getName()+'PAGE'+str(page)+'  Crawling FAILURE  RES '+str(res.status_code))
                return 2


def working(start_id=1):
    pros = get_pros(start_id)

    if pros:
        end_id = pros[len(pros)-1]['id']

        for i in range(len(pros)):
            print threading.currentThread().getName()+'  PROXY '+str(pros[i]['ip'])+'  ID '+str(pros[i]['id'])
            logging.info(threading.currentThread().getName()+'  PROXY '+str(pros[i]['ip'])+'  ID '+str(pros[i]['id']))
            while True:
                # 从queue中取出page 检查是否已被爬过，如果已被爬过:continue
                if rlock1.acquire():
                    if not queue.empty():
                        page = queue.get()
                        # visited_pages = ns.visited_pages
                        if page in visited_pages:
                            queue.task_done()
                            rlock1.release()
                            continue
                        visited_pages.add(page)
                        # ns.visited_pages = visited_pages
                        rlock1.release()

                        state = crawling(page, pros[i])
                        if state == 1:
                            continue
                        elif state == 2:
                            break
                        elif state == 0:
                            return
                    else:
                        rlock1.release()
                        continue
        working(end_id+1)
    else:
        return


visited_pages = set()
queue = Queue()

rlock1 = threading.RLock()
rlock2 = threading.RLock()
rlock3 = threading.RLock()


def main():
    start = datetime.now()
    queue.put(1)
    threads = []
    for i in range(3):
        t = threading.Thread(
            name='thread-'+str(i+1), target=working, args=(1,))
        t.setDaemon(True)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    finish = datetime.now()
    print '多线程用时: '+str(finish-start)


def test_web(page):
    c_start = datetime.now()
    try:
        res = requests_get(page)
        print res.status_code
    except Exception:
        logging.exception('message')
        print '  FAILURE --------------------------------------------------------------------------------'
    c_finish = datetime.now()
    print c_finish-c_start
