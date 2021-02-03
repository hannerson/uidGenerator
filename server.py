import os,sys
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.escape
from dbutils.pooled_db import PooledDB
import pymysql
from snowflake import *
from leafmysql import *

from tornado.options import define, options
define("port", default=8002, help="run on the given port", type=int)
pool = PooledDB(pymysql,6,host="10.0.29.8",user="autodms",passwd="yeelion",db="AutoDMS",charset="utf8",port=3306,cursorclass=pymysql.cursors.DictCursor)
conn = pool.connection()
cur = conn.cursor()
leaf = leafmysql(conn=conn, cur=cur, biz_tag="test")
leaf.startWorker()

class idsHandlerSnowflake(tornado.web.RequestHandler):
    def get(self):
        fmt = self.get_argument('fmt','idStr')
        resp = {}
        snowflake = snowflakeIds(1)
        if fmt == "dateStr":
            resp["id"] = "%s" % (snowflake.nextIdDate())
        else:
            resp["id"] = "%s" % (snowflake.nextId())
        self.write(resp)

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
    def get(self):
        resp = {}
        resp["id"] = "%s" % (leaf.nextId())
        self.write(resp)

    def post(self):
        resp = {}
        resp["id"] = "%s" % (leaf.nextId())
        self.write(resp)

if __name__ == "__main__":
    tornado.options.parse_command_line()
    app = tornado.web.Application(handlers=[(r"/getId", idsHandlerLeafmysql)])
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()

