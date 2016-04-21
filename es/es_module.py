#-*-coding:utf-8-*-
import PyBfdRedis
import sys
import time
import json
from elasticsearch import Elasticsearch
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from settings import *
import time 
#定义ES操作模块
class ESModule(object):

  def __init__(self, cid, index, doc_type):
    self.time_str = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    self.redis_client = PyBfdRedis.newClient('192.168.40.37:26379', 'bfdopen')
    self.es_client = Elasticsearch([{'host':'192.168.61.89','port':9200}], timeout=1000)
    self.cid = cid
    self.index = index
    self.doc_type = doc_type

  def __topn(self, items, n):
    result = {}
    total = 0
    for item in sorted(items, key=lambda x: x[1], reverse=True)[:n]:
      total += item[1]
      result.update({item[0]:item[1]})
    return (result,total)

  def __output2redis(self, key, value):
    if value['detail']:
      print key, PyBfdRedis.set(self.redis_client, key, json.dumps(value, ensure_ascii=False))

  def set_province(self, province_map, type='dmp_test'):
    '''
    设置人口属性中的地域分布
    '''
    query = '{"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"' + self.cid + '"}}]}}}}]}}},"size":0,"facets":{"province":{"terms":{"field":"dg_info.location.province","size":100}}}}'
    res = self.es_client.search(index ='%s' %self.index, doc_type = '%s' %self.doc_type, body=query, timeout=100000)
    value = {
        "update_time": self.time_str,
        "detail": {
        }
    }
    key = '%s:DMP:%s:bfd_province' %(self.cid, type)
    pTotal = 0
    for item in res['facets']['province']['terms']:
      if item['term'].encode('utf-8') in province_map:
        province = province_map[item['term'].encode('utf-8')]
        pTotal += item['count']
        value['detail'][province] = item['count']
    
    for item in value['detail']:
      value['detail'][item] = float('%0.2f' %((value['detail'][item]*1.0/pTotal)*100))
    self.__output2redis(key,value)

  
  def set_cons_allCategory(self, methods, bfd_categorys, type = 'dmp_test'):
    '''
    获取百分点全局品类分布
    '''
    query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}},{"term":{"bfd_category.method":"AddCart"}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid": self.cid}}]}}}}]}}},"size":0,"facets":{"first_category":{"nested":"bfd_category","terms":{"field":"first_category","size":100},"facet_filter":{"term":{"cid":"Cbaifendian"}}}}}
    res = self.es_client.search(index ='%s' %self.index, doc_type = '%s' %self.doc_type, body=query, timeout=100000)
    bfd_first_categorys = []
    for item in res['facets']['first_category']['terms']:
      bfd_first_categorys.append(item['term'])
    key = '%s:DMP:%s:bfd_ec_first_category' %(self.cid, type)
    value = {
      "update_time": self.time_str,
      "detail": {
      }
    }
    value_list = []
    #计算不同行为的总人数
    method_total = {}
    for method in methods:
      query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}},{"term":{"bfd_category.method": method}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid": self.cid}}]}}}}]}}},"size":0}
      res = self.es_client.search(index= '%s' %self.index, doc_type= '%s' %self.doc_type, body=query, timeout=100000)
      count = res['hits']['total']
      method_total.update({method:count})
    #计算不同品类下不同行为的人数
    for first_category, second_category in bfd_categorys.items():
      term = {'name' : first_category}
      for method in methods:
        query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}},{"term":{"bfd_category.first_category":first_category}},{"term":{"bfd_category.method":method}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid": self.cid}}]}}}}]}}},"size":0}
        res = self.es_client.search(index= '%s' %self.index, doc_type= '%s' %self.doc_type, body=query, timeout=100000)
        count = res['hits']['total']
        term.update({method:count})
      value_list.append(term)
    value_list = sorted(value_list, cmp=lambda x,y:cmp(x['AddCart'], y['Pay']))[10:]
    
    for item in value_list:
      value['detail'].update({item['name']:{'AddCart':float('%0.2f'%((item['AddCart']*1.0/ method_total['AddCart']) * 100)),'Pay':float('%0.2f'%((item['Pay']*1.0/method_total['Pay'])*100))}})
    self.__output2redis(key, value)
      
  def set_cons_categorys(self, brand_blacklist, bfd_categorys, type = 'dmp_test'):
    '''
    获取全网消费偏好二级类目排名以及品牌排行
    '''
    #全网消费偏好-二级类目排行
    for first_category, second_categorys in bfd_categorys.items():
      key = '%s:DMP:%s:bfd_category_rank:%s' % (self.cid, type, first_category)    
      value = {
        "update_time": self.time_str,
        "detail": {
        }
      }
      cTotal = 0
      item_list = []
      for second_category in second_categorys:
        query ={"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}},{"term":{"bfd_category.first_category":first_category}},{"term":{"bfd_category.second_category":second_category}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":self.cid}}]}}}}]}}},"size":0}
        res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=10000)
        count = res['hits']['total']
        if count == 0:
          continue
        cTotal += count
        item_list.append([second_category, count]) 
      (result,total) = self.__topn(item_list, 10)
      for item in result:
        v = float('%0.2f'%((result[item]*1.0/cTotal)*100))
        if v != 0.0:
          value['detail'][item] = v
      self.__output2redis(key,value)

      # 全局消费偏好->最受关注的品牌
      query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}},{"term":{"bfd_category.first_category": first_category}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid": self.cid}}]}}}}]}}},"size":0,"facets":{"brands":{"nested":"bfd_category","terms":{"field":"brand","size":30},"facet_filter":{"and":[{"term":{"cid":"Cbaifendian"}},{"term":{"first_category":first_category}}]}}}}
      res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=10000)
      brands = []
      for item in res['facets']['brands']['terms']:
        if item['term'].encode('utf-8') in brand_blacklist['all'] or item['term'].encode('utf-8') in brand_blacklist.get(first_category,[]):
          continue
        else:
          brands.append(item['term'])
      key = '%s:DMP:%s:bfd_category_brands:%s' % (self.cid, type, first_category)
      value = {
        "update_time": self.time_str,
        "detail": {
        }
      }
      bTotal = 0
      for brand in brands[:10]:
        query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}},{"term":{"bfd_category.first_category": first_category}},{"term":{"bfd_category.brand":brand}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid": self.cid}}]}}}}]}}},"size":0}
        res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=10000)
        count = res['hits']['total']
        bTotal += count
        value['detail'].update({brand:count})
      for item in value['detail']:
        value['detail'][item] = float('%0.2f'%((value['detail'][item]*1.0/bTotal)*100))
      self.__output2redis(key,value)

  def set_indus_categorys(self, industry, tag_blacklist, factor = 1.0, category_num = 10, num_threshold = 20, type = 'dmp_test'):
    '''
    行业兴趣偏好相关指标
    '''
    #行业兴趣偏好->全局->类目统计
    media_second_categorys = []
    query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}},{"term":{"bfd_category.type":"media"}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid": self.cid}}]}}}}]}}},"size":0,"facets":{"first_category":{"nested":"bfd_category","terms":{"field":"second_category","size":100},"facet_filter":{"and":[{"term":{"cid":"Cbaifendian"}},{"term":{"type":"media"}},{"term":{"first_category": industry}}]}}}}
    res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=10000)
    for item in res['facets']['first_category']['terms']:
      media_second_categorys.append(item['term'])
    sTotal = 0
    key = '%s:DMP:%s:bfd_media_first_category' %(self.cid, type)
    value = {
      "update_time": self.time_str,
      "detail": {
      }
    }
    cate_list = []
    for second_category in media_second_categorys:
      query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}},{"term":{"bfd_category.type":"media"}},{"term":{"bfd_category.first_category":industry}},{"term":{"bfd_category.second_category":second_category}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":self.cid}}]}}}}]}}},"size":0}
      res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=10000)
      count = res['hits']['total']
      if count == 0:
        continue
      sTotal += count
      cate_list.append([second_category, count])
      top_num = category_num
      if len(cate_list) < category_num:
        top_num = len(cate_list)
      (result, total) = self.__topn(cate_list, top_num)
    for item in result:
      v =  float('%0.2f'%((result[item]*1.0/sTotal*1.0)*100))
      if v != 0 :
        value['detail'].update({item:v})
    self.__output2redis(key,value)

    # 行业兴趣偏好->全局->用户关注热词
    key = '%s:DMP:%s:bfd_media_category_tag:_all_' %(self.cid, type)
    value = {
      "update_time": self.time_str,
      "detail": {
      }
    }
    query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":self.cid}}]}}}}]}}},"size":0,"facets":{"first_category":{"nested":"bfd_category","terms":{"field":"tag","size":100},"facet_filter":{"and":[{"term":{"cid":"Cbaifendian"}},{"or":[]},{'term':{'first_category':industry}},{"term":{"type":"media"}}]}}}}
    for second_category in media_second_categorys:
      query['facets']['first_category']['facet_filter']['and'][1]['or'].append({'term':{'second_category':second_category}})
    res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=10000)
    tags = []
    num = 0
    for item in res['facets']['first_category']['terms']:
      if len(item['term'].encode('utf-8').strip().split(',')) > 1:
        continue
      if item['term'].encode('utf-8') in tag_blacklist:
        continue
      tags.append(item['term'])
      num += 1
      if num == num_threshold:
        break
    tag_list = []
    for tag in tags:
      query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}},{"term":{"bfd_category.tag":tag}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":self.cid}}]}}}}]}}},"size":0}
      res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=100000)
      count = res['hits']['total']
      tag_list.append([tag,count])
    tag_list.sort(cmp=lambda x,y:cmp(x[1],y[1]))
    weight = factor
    for tag in tag_list:
      value['detail'].update({tag[0]:weight})
      weight = float('%0.2f' % (weight * 0.95))
    self.__output2redis(key,value)

    #行业兴趣偏好->一级类目->热词
    for second_category in media_second_categorys:
      query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}},{"term":{"bfd_category.first_category":industry}},{"term":{"bfd_category.second_category":second_category}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":self.cid}}]}}}}]}}},"size":0,"facets":{"tag":{"nested":"bfd_category","terms":{"field":"tag","size":100},"facet_filter":{"and":[{"term":{"cid":"Cbaifendian"}},{"term":{"first_category":industry}},{"term":{"second_category": second_category}}]}}}}
      res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=100000)
      tags = []
      key = '%s:DMP:%s:bfd_media_category_tag:%s' %(self.cid, type, second_category)
      value = {
        "update_time": self.time_str,
        "detail": {
        }
      }
      num = 0
      for item in res['facets']['tag']['terms']:
        if item['term'].encode('utf-8') in tag_blacklist:
          continue
        if len(item['term'].encode('utf-8').strip().split(',')) > 1:
          continue
        tags.append(item['term'])
        num += 1
        if num == num_threshold:
          break
      tag_list = []
      for tag in tags:
        query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}},{"term":{"bfd_category.first_category":industry}},{'term':{'bfd_category.second_category':second_category}},{"term":{"bfd_category.tag":tag}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":self.cid}}]}}}}]}}},"size":0}         
        res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=100000)
        count = res['hits']['total']
        if count == 0:
          continue
        tag_list.append([tag,count])
      tag_list.sort(cmp=lambda x,y:cmp(x[1],y[1]))
      weight = factor
      for tag in tag_list:
        value['detail'].update({tag[0]:weight})
        weight = float('%0.2f' % (weight * 0.95))
      self.__output2redis(key, value)
   
    # 行业兴趣偏好->一级类目->类目统计
    for second_category in media_second_categorys:
      query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}},{"term":{"bfd_category.first_category":industry}},{'term':{'bfd_category.second_category':second_category}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":self.cid}}]}}}}]}}},"size":0,"facets":{"second_category":{"nested":"bfd_category","terms":{"field":"third_category","size":100},"facet_filter":{"and":[{"term":{"cid":"Cbaifendian"}},{"term":{"type":"media"}},{"term":{"first_category":industry}},{'term':{'second_category':second_category}}]}}}}
      res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=100000)
      sTotal = 0
      key = '%s:DMP:%s:bfd_media_category_rank:%s' % (self.cid, type, second_category)
      value = {
        "update_time": self.time_str,
        "detail": {
        }
      }
      item_list = []
      for item in res['facets']['second_category']['terms']:
        third_category = item['term']
        query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}},{"term":{"bfd_category.first_category":industry}},{"term":{"bfd_category.second_category":second_category}},{'term':{'bfd_category.third_category':third_category}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":self.cid}}]}}}}]}}},"size":0}
        res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=100000)
        count = res['hits']['total']
        if count == 0:
          continue
        sTotal += count
        item_list.append([third_category, count])
      (result,total) = self.__topn(item_list, 10)
      for item in result:
        v = float('%0.2f'%((result[item]*1.0/sTotal)*100))
        if v != 0.0:
          value['detail'][item] = float('%0.2f'%((result[item]*1.0/sTotal)*100))
      self.__output2redis(key,value)

  def set_firstAllCategory(self, conf, num = 10, type = 'dmp_test'):
    '''
    第一方兴趣偏好-全局-品类分布
    '''
    key = '%s:DMP:%s:first_category' %(self.cid, type)
    value = { 
      "update_time": self.time_str,
      "detail": {
      }   
    }
    total = 0
    value_list = []
    for first_category in conf['FIRST_CATEGORYS']:
      subList = []
      if first_category in conf['COMBINATION']:
        for category in conf['COMBINATION'][first_category]:
          subList.append("{\"term\":{\"bfd_category.first_category\":\"%s\"}}" %category)
      subList.append("{\"term\":{\"bfd_category.first_category\":\"%s\"}}" %first_category)
      subStr = "[" + ",".join(subList) + "]" 
      query='{"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"' + self.cid + '"}}],"should":' + subStr + '}}}}]}}},"size":0}'
      res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=100000)
      count = res['hits']['total']
      value_list.append({'name':first_category,'value':count})
      total += count
    for item in value_list[:num]:
      v = float('%0.2f'%((item['value']*1.0/total)*100))
      if v != 0.0:
        value['detail'][item['name']] = v
    self.__output2redis(key, value)
  
  def set_firstAllHotWords(self, conf, tag_blacklist, num = 10, type = 'dmp_test'):
    '''
    第一方兴趣偏好-全局-用户关注热词
    '''
    query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":self.cid}}]}}}}]}}},"size":0,"facets":{"tag":{"nested":"bfd_category","terms":{"field":"tag","size":50},"facet_filter":{"and":[{"term":{"cid":self.cid}}]}}}}
    res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=100000)
    tags = []
    for item in res['facets']['tag']['terms']:
      if item['term'].encode('utf-8') in tag_blacklist:
        continue
      tags.append(item['term'])
    key = '%s:DMP:%s:category_tag:_all_'% (self.cid, type)
    value = {
      "update_time": self.time_str,
      "detail": {
      }
    }
    total = 0
    if len(tags) == 0:
      return
    tag_list = []
    for tag in tags[:num]:
      query ={"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":self.cid}},{"term":{"bfd_category.tag":tag}}]}}}}]}}},"size":0}
      res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=100000)
      count = res['hits']['total']
      tag_list.append([tag, count])
    tag_list.sort(cmp=lambda x,y:cmp(x[1],y[1]))
    weight = 1.0
    for tag in tag_list:
      value['detail'].update({tag[0]:weight})
      weight = float('%0.2f' % (weight * 0.95))
    self.__output2redis(key,value)


  def set_firstHotWords(self, conf, num, tag_blacklist, factor = 1.0, type = 'dmp_test'):
    '''
    设置全局第一方->类目->热词
    '''
    for first_category in conf['FIRST_CATEGORYS']:
      #查询一级类目下的tag标签
      query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":self.cid}}],"should":[]}}}}]}}},"size":0,"facets":{"tag":{"nested":"bfd_category","terms":{"field":"tag","size":50},"facet_filter":{"and":[{"term":{"cid":self.cid}},{"or":[]}]}}}}
      if first_category in conf['COMBINATION']:
        for category in conf['COMBINATION'][first_category]:
          query["query"]["filtered"]["filter"]["and"][1]["nested"]["filter"]["bool"]["should"].append({"term":{"bfd_category.first_category":category}})
          query["facets"]["tag"]["facet_filter"]["and"][1]["or"].append({"term":{"bfd_category.first_category":category}})
      query["query"]["filtered"]["filter"]["and"][1]["nested"]["filter"]["bool"]["should"].append({"term":{"bfd_category.first_category":first_category}})
      query["facets"]["tag"]["facet_filter"]["and"][1]["or"].append({"term":{"bfd_category.first_category":first_category}})
      res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=100000)
      tags = []
      for item in res['facets']['tag']['terms']:
        if item['term'].encode('utf-8') in tag_blacklist:
          continue
        tags.append(item['term'])
      tag_list = []
      if first_category in conf['ANOTHER']:
        key = '%s:DMP:%s:category_tag:%s' % (self.cid, type, conf['ANOTHER'][first_category])
      else:
        key = '%s:DMP:%s:category_tag:%s' % (self.cid, type, first_category)
      value = {
        "update_time": self.time_str,
        "detail": {
        }
      }
      if len(tags) == 0:
        continue
      #统计tag标签的个数
      for tag in tags[:num]:
        if first_category in conf['COMBINATION']:
          query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid": self.cid}},{"or":[]},{"term":{"bfd_category.tag": tag}}]}}}}]}}},"size":0}
          for category in conf['COMBINATION'][first_category]:
            query["query"]["filtered"]["filter"]["and"][1]["nested"]["filter"]["bool"]["must"][1]["or"].append({"term":{"bfd_category.first_category":category}})
          query["query"]["filtered"]["filter"]["and"][1]["nested"]["filter"]["bool"]["must"][1]["or"].append({"term":{"bfd_category.first_category":first_category}})
        else:
          query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":self.cid}},{"term":{"bfd_category.first_category": first_category}},{"term":{"bfd_category.tag": tag}}]}}}}]}}},"size":0}
        res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=100000)
        count = res['hits']['total']
        tag_list.append([tag,count])
      tag_list.sort(cmp=lambda x,y:cmp(x[1],y[1]))
      weight = factor
      for tag in tag_list:
        value['detail'].update({tag[0]:weight})
        weight = float('%0.2f' % (weight * 0.95))
      self.__output2redis(key, value)
        
  def set_firstCategorys(self, conf, type = 'dmp_test'):
    '''
    第一方兴趣偏好-全局-第一方内容偏好分析
    '''
    for first_category in conf['FIRST_CATEGORYS']:
      query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid": self.cid}}],"should":[]}}}}]}}},"size":0,"facets":{"second_category":{"nested":"bfd_category","terms":{"field":"second_category","size":10},"facet_filter":{"and":[{"term":{"cid": self.cid}},{"or":[]}]}}}}
      if first_category in conf['COMBINATION']:
        for category in conf['COMBINATION'][first_category]:
          query['query']['filtered']['filter']['and'][1]['nested']['filter']['bool']['should'].append({"term":{"bfd_category.first_category":category}})
          query["facets"]["second_category"]["facet_filter"]["and"][1]["or"].append({"term":{"bfd_category.first_category":category}})
      query['query']['filtered']['filter']['and'][1]['nested']['filter']['bool']['should'].append({"term":{"bfd_category.first_category":first_category}})
      query["facets"]["second_category"]["facet_filter"]["and"][1]["or"].append({"term":{"bfd_category.first_category":first_category}})
      res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=100000)
      second_categorys = []
      for item in res['facets']['second_category']['terms']:
        second_categorys.append(item['term'])
      if len(second_categorys) == 0:
        continue
      if first_category in conf['ANOTHER']:
        key = '%s:DMP:%s:category_rank:%s' % (self.cid, type, conf['ANOTHER'][first_category])
      else:
        key = '%s:DMP:%s:category_rank:%s' % (self.cid, type, first_category)
      value = {
        "update_time": self.time_str,
        "detail": {
        }
      }
      tmp_dic = {}
      total = 0
      for second_category in second_categorys:
        if first_category in conf['COMBINATION']:
          query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":self.cid}},{"or":[]},{"term":{"bfd_category.second_category": second_category}}]}}}}]}}},"size":0}
          for category in conf['COMBINATION'][first_category]:
            query["query"]["filtered"]["filter"]["and"][1]["nested"]["filter"]["bool"]["must"][1]["or"].append({"term":{"bfd_category.first_category":category}})
          query['query']['filtered']['filter']['and'][1]['nested']['filter']['bool']['must'][1]["or"].append({"term":{"bfd_category.first_category":first_category}})
        else:
          query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid": self.cid}},{"term":{"bfd_category.first_category": first_category}},{"term":{"bfd_category.second_category": second_category }}]}}}}]}}},"size":0}
        res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=100000)
        count = res['hits']['total']
        if count == 0:
          continue
        total += count
        tmp_category = second_category.encode('utf-8')
        if tmp_category in conf['SECOND_COMBINATION']:
          tmp_category = conf['SECOND_COMBINATION'][tmp_category]
        if tmp_category not in tmp_dic:
          tmp_dic.update({tmp_category:count})
        else:
          tmp_dic[tmp_category] = count + tmp_dic[tmp_category]
      for item in tmp_dic:
        v = float('%0.2f' %((tmp_dic[item]*1.0/total)*100))
        if v != 0.0:
          value['detail'][item] = v
      self.__output2redis(key, value)
  
  def set_hotCategorys(self, bfd_categorys, num = 20, type = 'dmp_test'):
    '''
    设置百分点标准品类热词
    '''
    for first_category,second_categorys, in bfd_categorys.items():
      key = '%s:DMP:%s:bfd_hot_category:%s' % (self.cid, type, first_category)
      value = { 
        'update_time': self.time_str,
        'detail': [
        ]   
      }
      query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}},{"term":{"bfd_category.first_category":first_category}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":self.cid}}]}}}}]}}},"size":0,"facets":{"third_category":{"nested":"bfd_category","terms":{"field":"third_category","size": num},"facet_filter":{"and":[{"term":{"cid":"Cbaifendian"}},{"term":{"first_category":first_category}}]}}}} 
      res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=100000)
      for item in res['facets']['third_category']['terms'][:num]:
        query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}},{"term":{"bfd_category.first_category":first_category}},{"term":{"bfd_category.third_category":item['term']}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":self.cid}}]}}}}]}}},"size":0}
        count = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=100000)['hits']['total']
        value['detail'].append({'name':item['term'], 'value':count})
      self.__output2redis(key, value)


  def set_hotBrands(self, bfd_categorys, num = 20, type = 'dmp_test'):
    '''
    设置百分点热品牌词
    '''
    for first_category, second_categorys in bfd_categorys.items():
      key = '%s:DMP:%s:bfd_hot_brand:%s' % (self.cid, type, first_category)
      value = {
        'update_time': self.time_str,
        'detail': [
        ]
      }
      query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}},{"term":{"bfd_category.first_category":first_category}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":self.cid}}]}}}}]}}},"size":0,"facets":{"brand":{"nested":"bfd_category","terms":{"field":"brand","size": num},"facet_filter":{"and":[{"term":{"cid":"Cbaifendian"}},{"term":{"first_category":first_category}}]}}}}
      res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=100000)
      for item in res['facets']['brand']['terms'][:num]:
        query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}},{"term":{"bfd_category.first_category":first_category}},{"term":{"bfd_category.brand":item['term']}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":self.cid}}]}}}}]}}},"size":0}
        count = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=100000)['hits']['total']
        if item['term'].encode('utf-8') in brand_blacklist['all'] or item['term'].encode('utf-8') in brand_blacklist.get(first_category,[]):
          continue
        value['detail'].append({'name':item['term'], 'value':count})
      self.__output2redis(key, value)
  
  def set_cont_allCategorys(self, bfd_medias, category_num = 10, type = 'dmp_test'):
    '''
    获取全网内容全品类偏好排名
    '''
    item_list = []
    total = 0
    for first_category in bfd_medias:
      query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}},{"term":{"bfd_category.first_category":first_category}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid": self.cid}}]}}}}]}}},"size":0}
      res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=100000)
      count = res['hits']['total']
      if count == 0:
        continue
      item_list.append([first_category, count])
      total += count
    if len(item_list) == 0:
      return
    cate_num = category_num 
    if len(item_list) < category_num:
      cate_num = len(item_list) 
    (result, subtotal) = self.__topn(item_list, cate_num)
    value = { 
      "update_time": self.time_str,
      "detail": {
      }   
    }
    key = '%s:DMP:%s:bfd_media_industry' %(self.cid, type)
    for item in result:
      v = float('%0.2f'%((result[item]*1.0/total)*100))
      if v != 0.0:
        value['detail'][item] = v
    self.__output2redis(key, value)
      

  def set_cont_categorys(self, bfd_medias, category_num = 10, tag_num = 20, type = 'dmp_test'):
    '''
    获取全网内容偏好二级类目排名以及品牌排行
    '''
    #获取媒体各个品类下二级类目排名
    for first_category, second_categorys in bfd_medias.items():
      item_list = []
      value = { 
        "update_time": self.time_str,
        "detail": {
        }   
      }

      key = '%s:DMP:%s:bfd_content_category:%s' % (self.cid, type, first_category)
      for second_category in second_categorys:
        query ={"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}},{"term":{"bfd_category.first_category":first_category}},{"term":{"bfd_category.second_category":second_category}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":self.cid}}]}}}}]}}},"size":0}
        res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=100000)
        count = res['hits']['total']
        if count == 0:
          continue
        item_list.append([second_category, count])
      if len(item_list) == 0:
        continue
      cate_num = category_num 
      if len(item_list) < category_num:
        cate_num = len(item_list) 
      #category_num = len(item_list)
      (result, total) = self.__topn(item_list, cate_num)
      for item in result:
        v = float('%0.2f'%((result[item]*1.0/total)*100))
        if v != 0.0:
          value['detail'][item] = v
      self.__output2redis(key, value)
      #获取媒体一级品类下对应的热词
      query = {"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}},{"term":{"bfd_category.first_category":first_category}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid": self.cid}}]}}}}]}}},"size":0,"facets":{"tag":{"nested":"bfd_category","terms":{"field":"tag","size":100},"facet_filter":{"and":[{"term":{"cid":"Cbaifendian"}},{"term":{"first_category":first_category}}]}}}}
      res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=100000)
      key = '%s:DMP:%s:bfd_content_hot_words:%s' % (self.cid, type, first_category)
      tag_list = []
      for item in res['facets']['tag']['terms']:
        count = item['count']
        if count == 0:
          continue
        tag = item['term'].encode('utf-8')
        if len(tag.strip().split(',')) > 1:
          continue
        tag_list.append([tag, count])
      if len(tag_list) == 0:
        continue
      value ={
        "update_time": self.time_str,
        "detail":{
        }
      }
      tmp_num = tag_num
      if len(tag_list) < tag_num:
        tmp_num = len(tag_list)
      (result, total) = self.__topn(tag_list, tmp_num)
      for item in result:
        v = float('%0.2f'%((result[item]*1.0/total)*100))
        if v != 0.0:
          value['detail'][item] = v
      self.__output2redis(key, value) 
        
      
             
    
  def set_firstAllPreferce(self, conf, tag_blacklist, num= 10, type = 'dmp_test'):
    '''
    设置第一方全局偏好
    '''
    #第一方兴趣偏好-全局-用户关注品类
    self.set_firstAllCategory(conf = conf, num = num, type = type)
    #第一方兴趣偏好-全局-用户关注热词
    self.set_firstAllHotWords(conf = conf, tag_blacklist = tag_blacklist, num = num, type = type)

  def set_cons_category_price(self, price_tab, factor = 0.5, type = 'dmp_test'):
    '''
    设置百分点电商品类价格区间
    '''
    priceMap = {}
    for category in price_tab:
      for pkey in price_tab[category]:
        itemset = price_tab[category][pkey]
        condition = "\"gt\": %f" %(float(itemset[0]))
        keyword = itemset[0] + "元"
        if len(itemset) > 1:
          condition += ",\"lt\": %f" %(float(itemset[1]))
          keyword += "-" + itemset[1] +"元"
        else:
          keyword += "以上"
        query = '{"query":{"filtered":{"filter":{"and":[{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"' + self.cid + '"}}]}}}},{"nested":{"path":"bfd_category","filter":{"bool":{"must":[{"term":{"bfd_category.cid":"Cbaifendian"}},{"range":{"bfd_category.average_price":{' + condition + '}}},{"term":{"bfd_category.first_category":"' + category + '"}}]}}}}]}}},"size":0}'
        res = self.es_client.search(index='%s' %self.index, doc_type='%s' %self.doc_type, body=query, timeout=100000)
        if category not in priceMap:
          priceMap[category] = {}
        priceMap[category].update({keyword:res['hits']['total']})
    key = '%s:DMP:%s:bfd_category_price:' %(self.cid, type)
    total = 0
    for category in priceMap:
      key = '%s:DMP:%s:bfd_category_price:%s' %(self.cid, type, category)
      value = { 
        "update_time": self.time_str,
        "detail": {}
      }
      levelMap = priceMap[category]
      for level in levelMap:
        if key == '母婴用品' and level == '0元-99元':
          total += levelMap[level]*factor
          value['detail'].update({level:levelMap[level]*factor})
        elif key == '出差旅游' and level == '0元-599元':
          continue
        elif key == '医疗保健' and level == '0元-29元':
          total += levelMap[level]*factor
          value['detail'].update({level:levelMap[level]*factor})
        else:
          total += levelMap[level]
          value['detail'].update({level:levelMap[level]})
      for item in value['detail']:
        value['detail'][item] = float('%0.2f' %((value['detail'][item]*1.0 / total) * 100))
      self.__output2redis(key, value)

  def set_cons_category_hotwords(self, bfd_categorys,factor = 0.5,brand_num = 5, search_num = 5, type = 'dmp_test'):
    '''
    设置标准第三方电商品类热词偏好
    '''
    key_brand = '%s:DMP:%s:bfd_hot_brand:' %(self.cid, type)
    key_category = '%s:DMP:%s:bfd_hot_category:' %(self.cid, type)
    key_search = 'All:DMP:dmp:bfd_hot_search_word:'

    for category in bfd_categorys:
      value ={
        "update_time": self.time_str,
        "detail":{
        }
      }
      total = 0
      hotwordDic = {}
      keyList = [key_category, key_brand]

      for key in keyList:
        key = key + category
        #读取redis数据
        valDic = json.loads(PyBfdRedis.get(self.redis_client,key))
        itemDic = {}
        for item in valDic['detail']:
          itemDic.update({item['name']:item['value']})
        itemList = sorted(itemDic.iteritems(),key=lambda d:d[1], reverse = True)
        if category == "文化娱乐":
          continue
        count = 1
        for item in itemList:
          if count > brand_num:
            break
          else:
            if item[0] not in hotwordDic:
              if item[0] == '万艾可' and category == '本地生活':
                continue
              hotwordDic.update({item[0]:item[1]})
              count += 1
              total += item[1]
      #热搜词
      key = key_search + category
      avag_val = total / (brand_num * 2.0)
      valDic = json.loads(PyBfdRedis.get(self.redis_client,key))
      itemList = sorted(valDic['detail'].iteritems(),key=lambda d:d[1], reverse = True)
      count = 1
      for item in itemList:
        if count > search_num:
          break
        else:
          if item[0] not in hotwordDic:
            if item[0] == '万艾可' and category == '本地生活':
              continue
            if category in search_black:
              if item[0].encode('utf-8') in search_black[category]:
                continue
            if avag_val > item[1]:
              hotwordDic.update({item[0]:(item[1] + avag_val * factor)})
              total += item[1] + avag_val * factor
            else:
              hotwordDic.update({item[0]:item[1]})
              total += item[1]
            count += 1
      for item in hotwordDic:
         value['detail'][item] = float('%0.2f' %((hotwordDic[item] * 1.5 / total) * 100))
      key = '%s:DMP:%s:bfd_category_tag:%s' %(self.cid, type, category)
      for item in hotwordDic:
        value['detail'][item] = float('%0.2f' %((hotwordDic[item] * 1.5 / total) * 100))
      self.__output2redis(key, value)
      


if __name__ == "__main__":
  cid = 'Ccaixingwang'
  type = 'dmp_test'
  index = 'dmp_%s' %cid.lower()
  doc_type = 'dmp'
  methods = ['AddCart','Pay']
  threshold = 20
  industry = CLIENT_CONF[cid]['INDUSTRY']
  es_inst = ESModule(cid, index, doc_type)
  es_inst.set_province(province_map = PROVINCE_MAP, type = type)
  es_inst.set_cons_allCategory(methods = methods, bfd_categorys = bfd_categorys, type = type)
  es_inst.set_cons_categorys(brand_blacklist = brand_blacklist, bfd_categorys = bfd_categorys, type = type)
  es_inst.set_indus_categorys(industry = industry, tag_blacklist = [], category_num = 10, num_threshold = threshold, type = type)
  es_inst.set_firstAllPreferce(conf = CLIENT_CONF[cid], tag_blacklist = [], num = 10, type = type)
  es_inst.set_firstAllCategory(conf = CLIENT_CONF[cid], num = 10, type = type)
  es_inst.set_firstAllHotWords(conf = CLIENT_CONF[cid], tag_blacklist = [], num = 20, type = type)
  es_inst.set_firstCategorys(conf = CLIENT_CONF[cid], type = type)
  es_inst.set_firstHotWords(conf = CLIENT_CONF[cid], type = type, num = 20, tag_blacklist = [])
  es_inst.set_hotCategorys(type = type, num = 20, bfd_categorys = bfd_categorys)
  es_inst.set_hotBrands(type = type, num = 20, bfd_categorys = bfd_categorys)
  es_inst.set_cont_categorys(type = type, category_num = 10, tag_num = 20, bfd_medias = bfd_medias)
  es_inst.set_cons_category_price(price_tab = price_level, type = type)
  es_inst.set_cons_category_hotwords(bfd_categorys = bfd_categorys)
