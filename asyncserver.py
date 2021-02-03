import os,sys
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.escape
import tornado.gen
from tornado.concurrent import run_on_executor
from concurrent.futures import ThreadPoolExecutor
from tornado.options import define, options
from dbutils.pooled_db import PooledDB
import pymysql
from snowflake import *
from leafmysql import *

pool = PooledDB(pymysql,6,host="10.0.29.8",user="autodms",passwd="yeelion",db="AutoDMS",charset="utf8",port=3306,cursorclass=pymysql.cursors.DictCursor)
conn = pool.connection()
cur = conn.cursor()
leaf = leafmysql(conn=conn, cur=cur, biz_tag="test")
leaf.startWorker()

define("port", default=8008, help="run on the given port", type=int)

class idsHandlerSnowflake(tornado.web.RequestHandler):
    max_thread_num = 10
    executor = ThreadPoolExecutor(max_workers=max_thread_num)
    @run_on_executor
    #@tornado.gen.coroutine
    def my_func(self):
        time.sleep(10)
        return 1

    @run_on_executor
    #@tornado.gen.coroutine
    def genIds(self):
        fmt = self.get_argument('fmt','idStr')
        resp = {}
        snowflake = snowflakeIds(1)
        if fmt == "dateStr":
            resp["id"] = "%s" % (snowflake.nextIdDate())
        else:
            resp["id"] = "%s" % (snowflake.nextId())
        #print (resp)
        time.sleep(10)
        return resp

    @tornado.web.asynchronous
    #@tornado.gen.engine #同步处理,不能及时响应
    @tornado.gen.coroutine
    def get(self):
        resp = yield self.genIds()
        self.write(resp)
        self.finish()

    def post(self):
        fmt = self.get_argument('fmt','idStr')
        resp = {}
        snowflake = snowflakeIds(1)
        if fmt == "dateStr":
            resp["id"] = "%s" % (snowflake.nextIdDate())
        else:
            resp["id"] = "%s" % (snowflake.nextId())
        self.write(resp)

class idsHandlerLeafmysql(tornado.web.RequestHandler):
    max_thread_num = 10
    executor = ThreadPoolExecutor(max_workers=max_thread_num)
    @run_on_executor
    #@tornado.gen.coroutine
    def my_func(self):
        time.sleep(10)
        return 1

    @run_on_executor
    #@tornado.gen.coroutine
    def genIds(self):
        resp = {}
        resp["id"] = "%s" % (leaf.nextId())
        return resp

    @tornado.web.asynchronous
    #@tornado.gen.engine #同步处理,不能及时响应
    @tornado.gen.coroutine
    def get(self):
        resp = yield self.genIds()
        self.write(resp)
        self.finish()

    def post(self):
        resp = yield self.genIds()
        self.write(resp)
        self.finish()

if __name__ == "__main__":
    tornado.options.parse_command_line()
    app = tornado.web.Application(handlers=[(r"/getId", idsHandlerLeafmysql)])
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()

