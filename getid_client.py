import grpc
from snowflake import *
import getid_pb2_grpc
import getid_pb2


def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = getid_pb2_grpc.GenIdStub(channel)
    response = stub.GetId(getid_pb2.GetRequest(type=2))
    print (response)

if __name__ == "__main__":
    run()
