import os
import time
import datetime
import traceback
import logging
from dbutils.pooled_db import PooledDB
import pymysql
import threading

###leafmysql
#CREATE DATABASE leaf
#CREATE TABLE `leaf_alloc` (
#  `biz_tag` varchar(128)  NOT NULL DEFAULT '', -- your biz unique name
#  `max_id` bigint(20) NOT NULL DEFAULT '1',
#  `step` int(11) NOT NULL,
#  `description` varchar(256)  DEFAULT NULL,
#  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
#  PRIMARY KEY (`biz_tag`)
#) ENGINE=InnoDB;
#insert into leaf_alloc(biz_tag, max_id, step, description) values('leaf-segment-test', 1, 2000, 'Test leaf Segment Mode Get Id')

class leafmysql(object):
    def __init__(self, conn, cur, biz_tag, table='leaf_alloc'):
        self.table = table
        self.conn = conn
        self.cur = cur
        self.step = 0
        self.biz_tag = biz_tag
        self.begin_id1 = 0
        self.current_id1 = 0
        self.max_id1 = 0
        self.begin_id2 = 0
        self.current_id2 = 0
        self.max_id2 = 0
        self.cond = threading.Condition()
        self.async_worker = None
        self.is_stop = False

    def nextId(self):
        self.cond.acquire()

        if (self.current_id1 - self.begin_id1) > (self.step * 0.1) and self.begin_id2 <= self.begin_id1:
            self.cond.notify()
        self.cond.release()

        if self.current_id1 == self.max_id1:
            if self.max_id2 > self.max_id1:
                #swap
                self.begin_id1,self.current_id1,self.max_id1 = self.begin_id2,self.current_id2,self.max_id2

        if self.current_id1 < self.max_id1:
            self.current_id1 += 1
            return self.current_id1
        return -1

    def nextSegment(self):
        if self.begin_id1 == 0 and self.begin_id2 == 0:
            ###get segment
            self.cond.acquire()
            self.mysqlSegment()
            self.begin_id1,self.current_id1,self.max_id1 = self.begin_id2,self.current_id2,self.max_id2
            self.cond.notify()
            self.cond.release()
        while not self.is_stop:
            self.cond.acquire()
            self.cond.wait()
            if self.is_stop:
                break
            self.mysqlSegment()
            #print (self.begin_id2,self.max_id2,self.current_id2)
            self.cond.release()

    def mysqlSegment(self):
        self.conn.begin()
        sql = '''UPDATE `%s` SET max_id=max_id+step WHERE biz_tag="%s"''' % (self.table, self.biz_tag)
        self.cur.execute(sql)
        sql = '''SELECT biz_tag, max_id, step FROM `%s` WHERE biz_tag="%s"''' % (self.table, self.biz_tag)
        cnt = self.cur.execute(sql)
        if cnt > 0:
            ret = self.cur.fetchone()
            self.max_id2 = ret["max_id"]
            self.begin_id2 = ret["max_id"] - ret["step"]
            self.current_id2 = ret["max_id"] - ret["step"]
        self.conn.commit()

    def startWorker(self):
        self.async_worker = threading.Thread(target = self.nextSegment, args = ())
        self.async_worker.start()

    def stop(self):
        self.is_stop = True
        self.cond.acquire()
        self.cond.notify()
        self.cond.release()

if __name__ == "__main__":
    pool = PooledDB(pymysql,6,host="10.0.29.8",user="autodms",passwd="yeelion",db="AutoDMS",charset="utf8",port=3306,cursorclass=pymysql.cursors.DictCursor)
    conn = pool.connection()
    cur = conn.cursor()
    leaf = leafmysql(conn=conn, cur=cur, biz_tag="test")
    leaf.startWorker()
    for i in range(12):
        print (leaf.nextId())
    leaf.stop()
    cur.close()
    conn.close()
