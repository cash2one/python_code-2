#-*-coding:utf-8-*-
import time
import sys
import MySQLdb
reload(sys)
sys.setdefaultencoding('utf-8')

class InsertMysql(object):
    def __init__(self, host,user,passwd,db,port):
        #MySql客户端
        self.host = host 
        self.user = user
        self.passwd = passwd
        self.db = db
        self.port = port
        try:
            self.conn=MySQLdb.connect(host=self.host,user=self.user,passwd=self.passwd,db=self.db,port=self.port)
            self.cur=self.conn.cursor()
            self.conn.set_character_set('utf8')
            self.cur.execute('SET NAMES utf8;')
            self.cur.execute('SET CHARACTER SET utf8;')
            self.cur.execute('SET character_set_connection=utf8;')
        except MySQLdb.Error,e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])
    
    #获取n天前时间戳
    def get_before_timestamp(self,day):
        date = self.timestamp_datetime(time.time())
        s = self.datetime_timestamp(date)
        return (s-3600*24*day)
    
    #时间戳转化为当天0点时间
    def timestamp_datetime(self,value):
        format = '%Y-%m-%d 00:00:00'
        value = time.localtime(value)
        dt = time.strftime(format, value)
        return dt

    #格式化时间转化为时间戳
    def datetime_timestamp(self,dt):
        time.strptime(dt, '%Y-%m-%d %H:%M:%S')
        s = time.mktime(time.strptime(dt, '%Y-%m-%d %H:%M:%S'))
        return int(s)

    #插入MySQL
    def output2Mysql(self,id,cid,timemark,category,value):
        try:
            values=[id,cid,timemark,category,str(value)]
            self.cur.execute('insert into USER_ANALYSIS(`id`,`cid`,`timemark`,`category`,`value`) values(%s,%s,%s,%s,%s)',values)
            self.conn.commit()
        except MySQLdb.Error,e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])


    def closeMysql(self):
         self.cur.close()
         self.conn.close() 


if __name__ == "__main__":
    host = '172.19.1.77'
    user = 'bdi'
    passwd = '123456'
    db = 'dbi'
    port = 3306
    es = InsertMysql(host,user,passwd,db,port)    
    day = es.get_before_timestamp(day=0) #0/7/15/30
    es.output2Mysql('lxw12','C17k111','7','人口属性','test')
    es.closeMysql();
    


