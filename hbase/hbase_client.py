#-*- coding=utf-8 -*-

# server addresses
HBHOST = "192.168.96.253:8080"
HBTABLE = "CleanOfflineUserProfileTempV1"
#HBTABLE = "Clean_data_manage_testV1"
import sys
sys.path.append("/opt/bre/usrTag/bfdopen/bfdopen")
from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from hbase import THBaseService
from hbase.ttypes import *
import json
from proto.UserProfile_pb2 import UserProfile
from utils.tools import proto2json

class HbaseClient:

    def __init__(self,host,table):
        host = host.split(':')
        self.port = int(host[1])
        self.host = host[0]
        self.table = table
        
        #初始化Thrift连接
        transport = TSocket.TSocket(self.host, self.port)    #建立socket连接
        #transport.setTimeout(5000)
        self.transport = TTransport.TBufferedTransport(transport)     #设置缓存，非常重要
        protocol = TBinaryProtocol.TBinaryProtocol(transport)    #包装在协议中
        self.client = THBaseService.Client(protocol)    #创建客户端使用规定的解码协议
        #连接成功
        print "Connect to {0}:{1}".format(self.host, self.port)
        transport.open()

    def __del__(self):
        print "Disconnect from {0}:{1}".format(self.host, self.port)
        self.transport.close()

    def get_column(self, rowkey, column):
        get = TGet(row = rowkey)
        print "Getting:", get
        #ret = self.client.get(self.table, rowkey, column, None)
        res = self.client.get(self.table, get)
        for resColumn in res.columnValues:
            up = UserProfile()
            up.ParseFromString(resColumn.value)
            result = proto2json(up)
            #print "value:{0}, type:{1}".format(,type(resColumn.value))
            print "value:{0}, family: {1}, qualifier:{2}, timestamp:{3}".format(json.dumps(result, ensure_ascii = False), 
                                                                                resColumn.family, 
                                                                                resColumn.qualifier,
                                                                                resColumn.timestamp)
    def get_Up(self, rowkey):
        '''从Hbase中获取单个用户画像的信息

        以up:all列为主，如果没有，获取up:列的信息, 如果没有返回None
        '''
        get = TGet(row = rowkey)
        res = self.client.get(self.table, get)
        res_map = {}
        for resColumn in res.columnValues:
            if resColumn.family == "up":
                if resColumn.qualifier not in res_map:
                    res_map.update({resColumn.qualifier:{}})
                if "timestamp" not in res_map[resColumn.qualifier] or res_map[resColumn.qualifier][resColumn.timestamp] < resColumn.timestamp:
                    res_map[resColumn.qualifier]["timestamp"] = resColumn.timestamp
                    res_map[resColumn.qualifier]["value"] = resColumn.value
        if len(res_map) == 0:
            return None
        else:
             if "all" in res_map:
                 print "The result is all."
                 return res_map["all"]["value"]
             elif "" in res_map:
                 print "The result is empty."
                 return res_map[""]["value"]
             else:
                 return None
 
        
    '''
    def get_value(self, rowkey, column):
        ret = self.client.get(self.table, rowkey.encode('utf8'), column, None)
        return ret
    '''

#def get_columns(key,columns,host=HBHOST,table=HBTABLE):
    #if (host,table) not in bfdhbase:
#    client = HbaseClient(host,table)
#    return client.get_column(key, columns)

if __name__ == "__main__":
    import sys
    client = HbaseClient(host = HBHOST, table = HBTABLE)
    if len(sys.argv) != 2:
        print "Usage: <gid>"
    else:
        gid = sys.argv[1]
        rowkey = gid[::-1]
        res = client.get_Up(rowkey)
        up = UserProfile()
        if res == None:
            print "The result is empty."
        else:
            up.ParseFromString(res)
            result = proto2json(up)
            print "The result: {0}".format(json.dumps(result, ensure_ascii = False))
