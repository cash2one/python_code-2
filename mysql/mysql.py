#coding:utf8

import MySQLdb
import sys
import time
import datetime

reload(sys)
sys.setdefaultencoding("utf8")

class MySql():

    def __init__(self,host,user,passwd,db,port,charset='utf8'):
        try:
            self.conn = MySQLdb.connect(host=host,user=user,passwd=passwd,db=db,port=port)
        except MySQLdb.Error,e:
            error_msg = 'Cannot connect to server\nERROR (%s): %s' %(e.args[0],e.args[1])
            print error_msg
            sys.exit()
        self.cursor = self.conn.cursor()

    def query(self,sql):
        count = self.cursor.execute(sql)
        rows = self.cursor.fetchall()

        print "----------------query sql------------------"
        print "%s"%(sql)
        print "----------------query result count---------------"
        print "%d"%(count)
        return rows

    def delete(self,sql):
        try:
            count = self.cursor.execute(sql)
            self.conn.commit()
            print "----------------delete sql------------------"
            print "%s"%(sql)
            print "----------------delete result count---------------"
            print "%d"%(count)
        except Exception as e:
            print "执行MYSQL: %s 时出错：%s".decode("utf-8") % (sql, e)
            self.conn.rollback()

    def update(self,sql,arr=None):
        try:
            #count = self.cursor.execute(sql)
            print "----------------update sql------------------"
            print "%s"%(sql)
            if arr != None:
                count=self.cursor.executemany(sql, arr)
            else:
                count=self.cursor.execute(sql)
            self.conn.commit()
            print "----------------update result count---------------"
            print "%d"%(count)
        except Exception as e:
            print "执行MYSQL: %s 时出错：%s".decode("utf-8") % (sql, e)
            self.conn.rollback()

    def insert(self,sql,arr=None):
        try:
            print "----------------insert sql------------------"
            print sql

            if arr != None:
                print "----------------insert datas------------------"
                print arr
                self.cursor.executemany(sql, arr)
            else:
                self.cursor.execute(sql)
            self.conn.commit()
            return 1
        except Exception as e:
            print "执行MYSQL: %s 时出错：%s".decode("utf-8") % (sql, e)
            self.conn.rollback()
            return 255

    def __del__(self):
        self.cursor.close()
        self.conn.close()



