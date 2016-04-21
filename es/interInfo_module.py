#!/usr/local/bin/python
#-*-coding:utf-8-*-

#import PyBfdRedis
import sys
import time
import json
from elasticsearch import Elasticsearch
reload(sys)
sys.setdefaultencoding('utf-8')
from settings import *
from make_query import Make_ES_Query

class InterInfo(object):

    def __init__(self, index, doc_type, query_prefix, total = 0):
        self.time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        #elasticsearch客户端
        self.es_client = Elasticsearch([{'host':'192.168.40.87','port':19200}], timeout=1000)
        self.index = index
        self.doc_type = doc_type
        #获取总数
        query = {"size":0}
        query.update(query_prefix)
        try:
            res = self.es_client.search(index ='%s' %self.index, doc_type = '%s' %self.doc_type,
                                        body=query, timeout=100000)
            self.total = res['hits']['total']
        except Exception, e:
            print e
            self.total = total

    def __topn(self, items, n):
        result = {}
        total = 0
        for item in sorted(items, key=lambda x: x[1], reverse=True)[:n]:
            result.update({item[0]:item[1]})
        return result

    '''
    def __output2redis(self, key, value):
        if value:
            print key, PyBfdRedis.set(self.redis_client, key, json.dumps(value))
    '''

    def dic2list(self,value):
        list = []
        for key in value:
            try:
                list.append({"name":key, "value":value[key]})
            except Exception, e:
                print "dic2list function raise Exception. "
                print e
        return list


    def get_oper_systems(self, argv): # argv --num,os_black,query_prefix,type
        '''
        获取PC端的操作系统属性
        '''
	num = argv[0]
	os_black = argv[1]
	query_prefix = argv[2]
	type = argv[3]

        #query = {"query":{"match_all":{}},"size":0,"facets":{"browser":{"nested":"inter_ft","terms":{"field":"inter_ft.terminal_types","size":10},"facet_filter":{"and":[{"term":{"inter_ft.channel":type}}]}}}}
        query = {"query":{"match_all":{}},"size":0,"facets":{"browser":{"nested":"inter_ft","terms":{"field":"inter_ft.oper_systems","size":10}}    }}

        try:
            res = self.es_client.search(index ='%s' %self.index, doc_type = '%s' %self.doc_type,
                                        body=query, timeout=100000)
            value = {}
            for items in res['facets']['browser']['terms']:
                oper_type = items['term'].encode('utf-8').strip()
                if oper_type in os_black:
                    oper_type = os_black[oper_type]
                value[oper_type] = int(items['count'])
        except Exception,e:
            print e
        return value


    def get_browser(self, argv): # argv --browser_black,num,query_prefix,type
        '''
        获取浏览器属性
        '''
	browser_black = argv[0]
	num = argv[1]
	query_prefix = argv[2]
	type = argv[3]

        query = {"query":{"match_all":{}},"size":0,"facets":{"browser":{"nested":"inter_ft","terms":{"field":"inter_ft.browsers","size":10},"facet_filter":{"and":[{"term":{"inter_ft.channel":type}}]}}}}

	value = {}
        try:
            res = self.es_client.search(index ='%s' %self.index, doc_type = '%s' %self.doc_type,
                                        body=query, timeout=100000)
	    internet_count = res['hits']['total']
            for items in res['facets']['browser']['terms']:
                browser = items['term'].encode('utf-8').strip()
                if browser in browser_black:
		    browser = browser_black[browser]
		value[browser] = int(items['count'])
        except Exception,e:
            print e
        return value


    def get_internet_time(self, argv): # argv --intervalMap,num,query_prefix,type
        '''
        设置上网时段属性
        '''
	intervalMap = argv[0]
	num = argv[1]
	query_prefix = argv[2]
	type = argv[3]

        query = {"query":{"match_all":{}},"size":0,"facets":{"browser":{"nested":"inter_ft","terms":{"field":"inter_ft.internet_time","size":10},"facet_filter":{"and":[{"term":{"inter_ft.channel":type}}]}}}}
	value = {}
        try:
            res = self.es_client.search(index ='%s' %self.index, doc_type = '%s' %self.doc_type,
                                        body=query, timeout=100000)
            for items in res['facets']['browser']['terms']:
                internet_time = items['term']
                if internet_time in intervalMap:
		    if intervalMap[internet_time] not in value:
		        value[intervalMap[internet_time]] = 0
		    value[intervalMap[internet_time]] += int(items['count'])
        except Exception,e:
            print e
        return value 


    def get_online_time(self, argv): # argv --onlineTimeMap, num, query_prefix, type
        '''
        设置上网时长属性
        '''
	onlineTimeMap = argv[0]
	num = argv[1]
	query_prefix = argv[2]
	type = argv[3]


        query = {"query":{"match_all":{}},"size":0,"facets":{"browser":{"nested":"inter_ft","terms":{"field":"inter_ft.online_time","size":10},"facet_filter":{"and":[{"term":{"inter_ft.channel":type}}]}}}}

        try:
            res = self.es_client.search(index ='%s' %self.index, doc_type = '%s' %self.doc_type,
                                        body=query, timeout=100000)
            online_dict = {}
            for items in res['facets']['browser']['terms']:
                online_time = items['term']
                if online_time in onlineTimeMap:
		    if onlineTimeMap[online_time] not in online_dict:
                        online_dict[onlineTimeMap[online_time]] = 0
                    online_dict[onlineTimeMap[online_time]] += items['count']
        except Exception,e:
            print e
        return online_dict 

    def get_frequency(self, argv): # argv --freqMap, query_prefix, num, type
        '''
        设置上网频次属性
        '''
        freqMap = argv[0]
	query_prefix = argv[1]
	num = argv[2]
	type = argv[3]

        query = {"query":{"match_all":{}},"size":0,"facets":{"browser":{"nested":"inter_ft","terms":{"field":"inter_ft.frequency","size":10},"facet_filter":{"and":[{"term":{"inter_ft.channel":type}}]}}}}
        try:
            res = self.es_client.search(index ='%s' %self.index, doc_type = '%s' %self.doc_type, body=query, timeout=100000)
            frequency_dict = {}
            for items in res['facets']['browser']['terms']:
                frequency = items['term']
                if frequency in freqMap:
		    if freqMap[frequency] not in frequency_dict:
			frequency_dict[freqMap[frequency]] = 0
	            frequency_dict[freqMap[frequency]] += items['count']
        except Exception, e:
            print e
        return frequency_dict 

    def get_terminal_type(self, argv): # argv --termTypeMap, num, type, query_prefix
        '''
        设置终端类型属性
        '''
        termTypeMap = argv[0]
	num = argv[1]
	type = argv[2]
	query_prefix = argv[3]

        query = {"query":{"match_all":{}},"size":0,"facets":{"browser":{"nested":"inter_ft","terms":{"field":"inter_ft.terminal_types","size":10},"facet_filter":{"and":[{"term":{"inter_ft.channel":query_prefix}}]}}}}
	value = {}
        try:
            res = self.es_client.search(index ='%s' %self.index, doc_type = '%s' %self.doc_type, body=query, timeout=100000)
            terminal_dict = {}
            for items in res['facets']['browser']['terms']:
                terminal = items['term'].encode('utf-8').strip()
                if terminal in termTypeMap:
		    if termTypeMap[terminal] not in terminal_dict:
                    	terminal_dict[termTypeMap[terminal]] = 0
	            terminal_dict[termTypeMap[terminal]] += items['count']
        except Exception, e:
            print e
        return terminal_dict 

    def get_access_way(self, argv): # argv --termTypeMap, num, type, query_prefix
        '''
        设置上网方式
        '''
	termTypeMap = argv[0]
	num = argv[1]
	type = argv[2]
	query_prefix = argv[3]

        query = {"query":{"match_all":{}},"size":0,"facets":{"browser":{"nested":"inter_ft","terms":{"field":"inter_ft.access_way","size":10},"facet_filter":{"and":[{"term":{"inter_ft.channel":query_prefix}}]}}}}
        try:
            res = self.es_client.search(index ='%s' %self.index, doc_type = '%s' %self.doc_type, body=query, timeout=100000)
            access_way_dict = {}
            access_way_count = res['hits']['total']
            for items in res['facets']['browser']['terms']:
		access_way_dict.update({items['term'].encode('utf-8').strip() : items['count']})
        except Exception, e:
            print e
        return access_way_dict 


if __name__ == "__main__":
 
    type = 'userprofile'
    index = 'show_userprofile_v1'
    num = 10

    query_prefix = {"query":{"filtered":{"query":{"match_all":{}}}}}  
    interInfo_ins = InterInfo(doc_type = type, index = index, query_prefix = query_prefix, total = 0)
    value = {}
    inter_type = 'PC'
    #inter_type = 'Mobile'

    #interInfo_ins.get_browser([{}, num, query_prefix,inter_type])

    #interInfo_ins.get_online_time((onlineTimeMap , query_prefix , num , inter_type))    

    #interInfo_ins.get_oper_systems([num,os_black, query_prefix, inter_type])

    #interInfo_ins.get_internet_time((intervalMap , query_prefix, num, inter_type))

    #interInfo_ins.get_frequency([freqMap,query_prefix,num,inter_type])

    #interInfo_ins.get_terminal_type([termTypeMap, num, query_prefix, inter_type])
 
    #interInfo_ins.get_access_way(termTypeMap, num, query_prefix, inter_type))


    if len(interInfo_ins.get_browser([{}, num, query_prefix, inter_type])) > 0:
        value.update({"browser" : interInfo_ins.get_browser([{}, num, query_prefix,inter_type])})

    if len(interInfo_ins.get_oper_systems([num, os_black, query_prefix, inter_type])) > 0:
        value.update({"oper_systerms" : interInfo_ins.get_oper_systems([num,os_black, query_prefix, inter_type])})

    if len(interInfo_ins.get_internet_time((intervalMap, num,  query_prefix, inter_type))) > 0:
        value.update({"internet_time" : interInfo_ins.get_internet_time((intervalMap, num, query_prefix, inter_type))})

    if len(interInfo_ins.get_online_time((onlineTimeMap, num, query_prefix, inter_type))) > 0:
        value.update({"online_time" : interInfo_ins.get_online_time((onlineTimeMap, num, query_prefix, inter_type))})

    if len(interInfo_ins.get_frequency([freqMap,query_prefix,num,inter_type])) > 0:
        value.update({"frequency" : interInfo_ins.get_frequency([freqMap,query_prefix,num,inter_type])})

    if len(interInfo_ins.get_terminal_type([termTypeMap, num, query_prefix, inter_type])) > 0:
        value.update({"terminal_type" : interInfo_ins.get_terminal_type([termTypeMap, num, query_prefix, inter_type])})

    if len(interInfo_ins.get_access_way([termTypeMap, num, query_prefix, inter_type])) > 0:
        value.update({"access_way" : interInfo_ins.get_access_way([termTypeMap, num, query_prefix, inter_type])})
    print json.dumps(value, ensure_ascii=False)
