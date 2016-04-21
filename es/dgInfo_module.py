#!/usr/local/bin/python
#-*-coding:utf-8-*-
import sys
import time
import json
import random
from elasticsearch import Elasticsearch
reload(sys)
sys.setdefaultencoding('utf-8')
from settings import *
from make_query import Make_ES_Query

class DgInfo(object):


    def __init__(self,index, doc_type, query_prefix, total = 0):
        self.time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        #elasticsearch客户端
        self.es_client = Elasticsearch([{'host':'192.168.40.87','port':19200}], timeout=1000)
        self.index = index
        self.doc_type = doc_type
        #获取总数
        query = {"size":0}
        query.update(query_prefix)
        res = self.es_client.search(index ='%s' %self.index, doc_type = '%s' %self.doc_type, body=query, timeout=100000)
        if res['hits']['total'] == 0:
            self.total = total
        else:
            self.total = res['hits']['total']

    def __topn(self, items, n):
        result = {}
        total = 0
        for item in sorted(items, key=lambda x: x[1], reverse=True)[:n]:
            total += item[1]
            result.update({item[0]:item[1]})
        return (result,total)


    def get_internet_sex(self, argv):
        '''
        设置互联网性别属性
        '''
	query_prefix = argv[0]
        sexMap = argv[1]
        value = {}
        query = {"size":0,"facets":{"sex":{"terms":{"field":"dg_info.internet_sex","size":10}}}}
        try:
            res = self.es_client.search(index ='%s' %self.index, doc_type = '%s' %self.doc_type, \
                                        body=query, timeout=100000)
	    total = res['hits']['total']
            for items in res['facets']['sex']['terms']:
                try:
                    value.update({sexMap[items['term']]:int(items['count'])})
                except Exception, e:
                    continue
        except Exception, e:
            print e
        return value

    def get_natural_sex(self, argv):
        '''
        设置自然性别属性
        '''
	query_prefix = argv[0]
        value = {}
        query = {"size":0,"facets":{"sex":{"terms":{"field":"dg_info.natural_sex","size":10}}}}
        try:
            res = self.es_client.search(index ='%s' %self.index, doc_type = '%s' %self.doc_type, \
                                     body=query, timeout=100000)
	    total = res['hits']['total']
            for items in res['facets']['sex']['terms']:
                try:
                    value.update({sexMap[items['term']] : int(items['count'])})
                except Exception, e:
                    continue
        except Exception, e:
            print e
        return value

    def get_province(self, argv): # argv --query_prefix,province_map
        '''
        设置人口属性中的省地域分布
        '''
	query_prefix = argv[0]
	province_map = argv[1]
        query = {"size":0,"facets":{"province":{"terms":{"field":"dg_info.province","size":20}}}}
        value = {}
        try:
            res = self.es_client.search(index ='%s' %self.index, doc_type = '%s' %self.doc_type, \
                                         body=query, timeout=100000)
            for items in res['facets']['province']['terms']:
                if items['term'].encode('utf-8') in province_map:
                    try:
                        value.update({province_map[items['term'].encode('utf-8')] : items['count']})
                    except Exception, e:
                        continue
        except Exception, e:
            print e
        return value

    def get_city(self, argv): # argv --query_prefix
        '''
        设置人口属性中的城市地域分布
        '''
	query_prefix = argv[0]
        query = {"size":0,"facets":{"city":{"terms":{"field":"dg_info.city","size":20}}}}
        value = {}
        if self.total == 0:
            return {}
        try:
            res = self.es_client.search(index ='%s' %self.index, doc_type = '%s' %self.doc_type,  \
                                         body=query, timeout=100000)
            for items in res['facets']['city']['terms']:
                value.update({items['term'].encode('utf-8') : items['count']})
        except Exception, e:
            print e
        return value

    def get_internet_age(self, argv): # args --query_prefix,ageMap
        '''
        设置互联网年龄属性
        '''
	query_prefix = argv[0]
	ageMap = argv[1]
        query = {"size":0,"facets":{"age":{"terms":{"field":"dg_info.internet_age","size":10}}}}
        value = {}
        try:
            res = self.es_client.search(index ='%s' %self.index, doc_type = '%s' %self.doc_type, \
                                        body=query, timeout=100000)
            for items in res['facets']['age']['terms']:
                if items['term'] in ageMap:
                    if ageMap[items['term']] not in value:
                        value[ageMap[items['term']]] = 0
                    value[ageMap[items['term']]] += int(items['count'])
        except Exception, e:
            print e
        return value

    def get_natural_age(self, args): # args --query_prefix,ageMap
        '''
        设置自然年龄属性
        '''
	query_prefix = args[0]
	ageMap = args[1]
        query = {"size":0,"facets":{"age":{"terms":{"field":"dg_info.natural_age","size":10}}}}
        value = {}
        try:
            res = self.es_client.search(index ='%s' %self.index, doc_type = '%s' %self.doc_type, \
                                        body=query, timeout=100000)
            for items in res['facets']['age']['terms']:
                if items['term'] in ageMap:
                    if ageMap[items['term']] not in value:
                        value[ageMap[items['term']]] = 0
                    value[ageMap[items['term']]] += int(items['count'])
        except Exception, e:
            print e
        return value

    def get_marriage(self, argv):
        '''
        设置婚姻属性
        '''
	query_prefix = argv[0]
        marriageMap = argv[1]
        query = {"size":0,"facets":{"marriage":{"terms":{"field":"dg_info.marriage","size":10}}}}
        value = {}
        try:
            res = self.es_client.search(index ='%s' %self.index, doc_type = '%s' %self.doc_type, \
                                        body=query, timeout=100000)
            for items in res['facets']['marriage']['terms']:
                try:
                    value.update({marriageMap[items['term']] : int(items['count'])})
                except Exception, e:
                    print e
                    continue
        except Exception, e:
            print e
        return value

    def get_children(self, argv):  # query_prefix
        '''
        设置孩子属性
        '''
	query_prefix = argv[0]
        childrenMap = argv[1]
        query = {"size":0,"facets":{"children":{"terms":{"field":"dg_info.children","size":10}}}}
        value = {}
        try:
            res = self.es_client.search(index ='%s' %self.index, doc_type = '%s' %self.doc_type, \
                                        body=query, timeout=100000)
            value = {}
            for items in res['facets']['children']['terms']:
                try:
                    value.update({childrenMap[items['term']] : int(items['count'])})
                except Exception, e:
                    print e
                    continue
        except Exception, e:
            print e

        return value

    def get_level(self, argv): # argv --query_prefix
        '''
        设置城市类型
        '''
        query_prefix = argv[0]
        query = {"size":0,"facets":{"level":{"terms":{"field":"dg_info.level","size":10}}}}
        value = {}
        try:
            res = self.es_client.search(index ='%s' %self.index, doc_type = '%s' %self.doc_type, \
                                        body=query, timeout=100000)
            value = {}
            for items in res['facets']['level']['terms']:
                value.update({items['term'] : items['count']})
        except Exception, e:
            print e
        return value

if __name__ == "__main__":
    type = 'userprofile'
    index = 'bfd_userprofile_v1'
    num = 10

    query_prefix = {"query":{"filtered":{"query":{"match_all":{}}}}} 
    dg_ins = DgInfo(doc_type = type, index = index, query_prefix = query_prefix)
    value = {}
    #自定义任务

    if len(dg_ins.get_internet_sex((query_prefix, sexMap))) > 0:
        value.update({"internet_sex" : dg_ins.get_internet_sex((query_prefix, sexMap))})

    if len(dg_ins.get_natural_sex([query_prefix])) > 0:
        value.update({"natural_sex" : dg_ins.get_natural_sex([query_prefix])})

    if len(dg_ins.get_province((query_prefix, PROVINCE_MAP))) > 0:
        value.update({"province" : dg_ins.get_province((query_prefix, PROVINCE_MAP))})

    if len(dg_ins.get_city([query_prefix])) > 0:
        value.update({"city" : dg_ins.get_city([query_prefix])})

    if len(dg_ins.get_internet_age((query_prefix, ageMap))) > 0:
        value.update({"internet_age" : dg_ins.get_internet_age((query_prefix, ageMap))} )

    if len(dg_ins.get_natural_age((query_prefix, ageMap))) > 0:
        value.update({"natural_age" : dg_ins.get_natural_age((query_prefix, ageMap))} )

    if len(dg_ins.get_marriage((query_prefix, marriageMap))) > 0:
        value.update({"marriage" : dg_ins.get_marriage((query_prefix, marriageMap))})

    if len(dg_ins.get_children((query_prefix, childrenMap))) > 0:
        value.update({"children" : dg_ins.get_children((query_prefix, childrenMap))})

    if len(dg_ins.get_level([query_prefix])) > 0:
        value.update({"level" : dg_ins.get_level([query_prefix])})

    print json.dumps(value, ensure_ascii = False)
