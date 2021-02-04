from concurrent import futures
import grpc
import getid_pb2_grpc
import getid_pb2
from dbutils.pooled_db import PooledDB
import pymysql
from leafmysql import *
from snowflake import *

#pool = PooledDB(pymysql,6,host="10.0.29.8",user="autodms",passwd="yeelion",db="AutoDMS",charset="utf8",port=3306,cursorclass=pymysql.cursors.DictCursor)
#conn = pool.connection()
#cur = conn.cursor()
#leaf = leafmysql(conn=conn, cur=cur, biz_tag="test")
#leaf.startWorker()

class GenIdServicer(getid_pb2_grpc.GenIdServicer):
    def GetId(self, request, context):
        sf = snowflakeIds(1)
        if request.type == 1:
            sfId = sf.nextId()
        else:
            sfId = sf.nextIdDate()
        return getid_pb2.IdReply(id="%s" % sfId)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    getid_pb2_grpc.add_GenIdServicer_to_server(GenIdServicer(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("grpc server start...")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
