#-*-coding:utf-8-*-
from pymongo import MongoClient
import tornado.ioloop
import tornado.web
import json
import copy
import time
import sys
from utils.pylog import mylogger as logger
import datetime
import time
from elasticsearch import Elasticsearch
import requests
from multiprocessing import Process
import redis
reload(sys)
sys.setdefaultencoding('utf-8')
client = MongoClient('192.168.40.87', 27037)
es_client = Elasticsearch([{'host':'192.168.61.89','port':9200}])
redis_client = redis.Redis(host='192.168.49.114', port=6379, db=9)
MONGO_DBNAME_PREFIX='UserAction_'
ELASTICSERACH_INDEX_PREFIX='dmp_'
STATE_RUNNING = "RUNNING"
STATE_FINISHED = "FINISHED"
STATE_FAILED = "FAILED"
REDIS_CHANNEL = "DmpJobMsgQueue"
JOB_INFO={
    "groupid":"",
    "progress":"0%",
    "step":[
    ]
}
JOB_NAME_DELETE_OLD_GROUP="Delete Old Group Info"
JOB_NAME_CREATE_NEW_GROUP="Create New Group"
JOB_STEP_DELETE_OLD_GROUP={
    "name":JOB_NAME_DELETE_OLD_GROUP,
    "state":"",
    "progress":"0%"
}
JOB_STEP_CREATE_NEW_GROUP={
    "name":JOB_NAME_CREATE_NEW_GROUP,
    "state":"",
    "progress":"0%"
}
QUERY_ES_PATH_MAPPING={
    'sex':'dg_info.sex',
    'location':'dg_info.location.province',
    'age':'dg_info.age',
    'price_level':'market_info.price_level',
    'life_stage':'dg_info.life_stage',
    'marriage':'dg_info.marriage',
    'children':'dg_info.children'
}
QUERY_ES_NESTED_PATH_MAPPING={
    "internet_time":"inter_ft.internet_time",
    "terminal_types":"inter_ft.terminal_types",
    "ec_preference":"bfd_category",
    "media_preference":"bfd_category"
}
ES_NESTED_TEMPLATE={
    "nested":{
        "path":"",
        "filter":{
            "bool":{
            }
        }
    }
}
ES_QUERY_TEMPLATE={
    "query": {
        "filtered": {
            "filter": {
                "and": [
                    
                ]
            }
        }
    },
    "size": 0
}
def update_job_state(job_info, job_name, state, progress="0%"):
    for step in job_info["step"]:
        if step["name"] == job_name:
            step["state"] = state
            step["progress"] = progress
def create_group(query, cid, groupid, update_group=False):
    index_name = ELASTICSERACH_INDEX_PREFIX+cid.lower()
    collection_groupinfo = client[MONGO_DBNAME_PREFIX+cid]['group_info']
    job_info = copy.deepcopy(JOB_INFO)
    job_info["groupid"] = groupid
    if update_group:
        logger.debug('update group: [cid: %s] [groupid: %s] ' % (cid, groupid))
        job_step = copy.deepcopy(JOB_STEP_DELETE_OLD_GROUP)
        job_step["groupid"] = groupid
        job_step["state"] = STATE_RUNNING
        job_info["step"].append(job_step)
        logger.debug("send message to redis [channel: %s] [message: %s]" % (REDIS_CHANNEL, json.dumps(job_info)))
        redis_client.publish(REDIS_CHANNEL, json.dumps(job_info))
        collection_groupinfo.update({'groupid':groupid}, {'$pull':{'groupid':groupid}}, multi=True)
        update_job_state(job_info, JOB_NAME_DELETE_OLD_GROUP, STATE_FINISHED, "100%")
        logger.debug("send message to redis [channel: %s] [message: %s]" % (REDIS_CHANNEL, json.dumps(job_info)))
        redis_client.publish(REDIS_CHANNEL, json.dumps(job_info))
    logger.debug('create group: [cid: %s] [groupid: %s] ' % (cid, groupid))
    query.update({"fields":["gid"]})
    req = requests.post("http://192.168.61.89:9200/%s/dmp/_search/?search_type=scan&scroll=10m&size=500"%index_name,json.dumps(query))
    scroll_id = json.loads(req.content).get("_scroll_id")
    job_step = copy.deepcopy(JOB_STEP_CREATE_NEW_GROUP)
    job_step["state"] = STATE_RUNNING
    current_num = 0
    job_info["step"].append(job_step)
    logger.debug("send message to redis [channel: %s] [message: %s]" % \
        (REDIS_CHANNEL, json.dumps(job_info)))
    redis_client.publish(REDIS_CHANNEL, json.dumps(job_info))

    while True:
        total_req = requests.post("http://192.168.61.89:9200/_search/scroll/?scroll=10m",scroll_id)
        hits = json.loads(total_req.content).get("hits")["hits"]
        total = json.loads(total_req.content).get("hits")["total"]
        gid_list = []
        for i in hits:
            gid_list.append(i["_id"])
        current_num += len(gid_list)
   
        logger.debug("%d %d" % (current_num, total))
        if total == 0:
            progress = "0%"
        else:
            progress = "%d%%" % (((current_num * 1.0) / total) * 100)
        job_info["progress"] = progress
        update_job_state(job_info,JOB_NAME_CREATE_NEW_GROUP, STATE_RUNNING, progress)
        collection_groupinfo.update({'gid':{'$in':gid_list}}, {'$addToSet':{'groupid':groupid}},multi=True)
        if total == current_num:
            update_job_state(job_info, JOB_NAME_CREATE_NEW_GROUP, STATE_FINISHED, "100%")
            job_info["progress"] = "100%"
            logger.debug("send message to redis [channel: %s] [message: %s]" % \
                (REDIS_CHANNEL, json.dumps(job_info)))
            redis_client.publish(REDIS_CHANNEL, json.dumps(job_info))
            key = 'job_info:%s' % groupid
            ret = redis_client.set(key, json.dumps(job_info))
            logger.debug("save job info to redis [key: %s] [ret: %d]" % (key, ret))
            break

        logger.debug("send message to redis [channel: %s] [message: %s]" % (REDIS_CHANNEL, json.dumps(job_info)))
        redis_client.publish(REDIS_CHANNEL, json.dumps(job_info))

class BasicHandler(tornado.web.RequestHandler):
    def set_default_header(self):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header('Access-Control-Max-Age', 1000)
        self.set_header('Access-Control-Allow-Headers', '*')
        self.set_header('Content-type', 'application/json')
class DmpUserFilterHandlerV2(BasicHandler):
    def update_userprofile_query(self, query, state):
        if 'or' in query:
            result = {'or':[]}
            for item in query['or']:
                val = self.update_userprofile_query(item, state)
                result['or'].append(val)
            return result 
        elif 'and' in query:
            result = {'and':[]}
            for item in query['and']:
                val = self.update_userprofile_query(item, state)
                result['and'].append(val)
            return result
        elif 'filter' in query:
            for k,v in query['filter'].items():
                result = {}
                if k in ['sex', 'age',  'location', 'price_level', 'marriage', 'children', 'life_stage']:
                    if type(v) is dict and 'in' in v:
                        result.update({"or":[]})
                        for item in v['in']:
#result["or"].append({"term":{QUERY_ES_PATH_MAPPING[k]:item}})
                            if item  == "unknown":
                                result["or"].append({"not":{"exists":{"field":QUERY_ES_PATH_MAPPING[k]}}})
                            else:
                                result["or"].append({"term":{QUERY_ES_PATH_MAPPING[k]:item}})
                    elif type(v) is string:
                        result.update({"term":{QUERY_ES_PATH_MAPPING[k]:item}})
                elif k in ['terminal_types', 'internet_time']:
                    if type(v) is dict and 'in' in v:
                        result.update({"or":[]})
                        for item in v["in"]:
                            val = copy.deepcopy(ES_NESTED_TEMPLATE)
                            val["nested"]["path"] = QUERY_ES_NESTED_PATH_MAPPING[k]
                            val["nested"]["filter"]["bool"].update({"must":[{"term":\
                                {"%s.value"%QUERY_ES_NESTED_PATH_MAPPING[k]: item}}]})
                            result["or"].append(val)
                    elif type(v) is string:
                        val = copy.deepcopy(ES_NESTED_TEMPLATE)
                        val["nested"]["path"] = QUERY_ES_NESTED_PATH_MAPPING[k]
                        val["nested"]["filter"]["bool"].update({"must":[{"term":\
                            {"%s.value"%QUERY_ES_NESTED_PATH_MAPPING[k]: v}}]})
                        result = val 
                elif k in ['ec_preference', 'media_preference']:
                    if type(v) is dict and 'in' in v:
                        result.update({"or":[]})
                        for item in v["in"]:
                            val = copy.deepcopy(ES_NESTED_TEMPLATE)
                            val["nested"]["path"] = QUERY_ES_NESTED_PATH_MAPPING[k]
                            val["nested"]["filter"]["bool"].update({"must":[]})
                            for inner_k, inner_v in item.items():
                                if inner_k in ["first_category", "second_category", "third_category"]:
                                    val["nested"]["filter"]["bool"]["must"].append({"term":\
                                        {"%s.%s"%(QUERY_ES_NESTED_PATH_MAPPING[k],inner_k):inner_v}})
                            result["or"].append(val)
                    if type(v) is dict and 'in' not in v:
                        for inner_k, inner_v in v.items():
                            if inner_k in ["first_category", "second_category", "third_category"]:
                                val["nested"]["filter"]["bool"]["must"].append({"term":\
                                    {"%s.%s"%(QUERY_ES_NESTED_PATH_MAPPING[k],inner_k):inner_v}})

                return result 
            else:
                return None
    def update_useraction_query(self, query, state):
        if 'or' in query:
            result = {'or':[]} 
            for item in query['or']:
                val = self.update_useraction_query(item,state)
                result['or'].append(val)
            return result 
        elif 'and' in query:
            result = {'and':[]}
            for item in query['and']:
                val = self.update_useraction_query(item, state)
                result['and'].append(val)
            return result
        elif 'filter' in query:
            filter = query['filter']
            result = []
            if 'cid' not in filter:
                cid = 'undefine'
            else:
                cid = filter['cid']
            reverse = False
#logger.debug('filter : %s, %s' % (json.dumps(filter['flag']), ))
            if ('flag' in filter and filter['flag']  == True) or ('reverse' in filter and filter['reverse'] == True):
                reverse = True
            else:
                reverse = False
            time_range_val = {}
            for k, v in filter.items():
                if k in ['method', 'first_category', 'second_category', 'third_category']:
                    result.append({'term':{'useraction.%s' % k : v}})
                if k == 'time' or k == 'update_time':
                    tmp = {'range':{'useraction.update_time':{}}}
                    for k, v in filter['time'].items():
                        time_str = time.strftime("%Y-%m-%d", time.localtime(filter['time'][k]))
                        if cid == 'demo':
                            day_offset = (datetime.datetime.now() - datetime.datetime(2014,12,01)).days
                            time_str = time.strftime("%Y-%m-%d", \
                                 time.localtime(int(filter['time'][k]) - day_offset * 3600 * 24))
                        tmp['range']['useraction.update_time'].update({k:time_str})
                    time_range_val = copy.deepcopy(tmp)
                    result.append(tmp)

            val = copy.deepcopy(ES_NESTED_TEMPLATE)
            val["nested"]["path"] = "useraction"
            val['nested']['filter']['bool'].update({'must':result})
            if reverse:
                time_val = copy.deepcopy(ES_NESTED_TEMPLATE)
                time_val['nested']['path'] = 'useraction'
                time_val['nested']['filter']['bool'].update({'must':[time_range_val]})
                result = {'and':[{'not':val}, time_val]}
            else:
                result = val
            if not 'cid' in state:
                state['cid'] = cid
            if not cid == state['cid']:
                state['success'] = False
        return result
        
    def post(self):
        count = 1000
        query_str = self.request.body 
        logger.debug('query_str: %s'% query_str)
        state = {'success':True}
        if not query_str:
            self.write('{"code":1501,"msg":"post data should not be empty."}')
            return 
        query_obj = json.loads(query_str)
        query = query_obj.get('query', {})
        es_query = copy.deepcopy(ES_QUERY_TEMPLATE)
        if not query:
            self.write('{"code":1502,"msg":"param query is required."}')
            return 
        useraction = query.get('useraction', {})
        if useraction:
            result = self.update_useraction_query(useraction, state)
            es_query['query']['filtered']['filter']['and'].append(result)
        userprofile = query.get('userprofile', {})
        if userprofile:
            result = self.update_userprofile_query(userprofile, state)
            es_query['query']['filtered']['filter']['and'].append(result)
            
        if 'cid' in query:
            state['cid'] = query['cid']
        index_name = ELASTICSERACH_INDEX_PREFIX+state['cid'].lower()
        if not es_query["query"]["filtered"]["filter"]["and"] :
            es_query = {"query":{"filtered":{"filter":{}}}, "size":0}
        logger.debug(json.dumps(es_query))
        res = es_client.search(index=index_name, doc_type='dmp', body=es_query, timeout=100000)
        count = res['hits']['total']
        result = {'code':1000, 'msg':'OK', 'result':{'count':count}}
        if 'groupid' in query_obj:
            if 'update_group' in query_obj and query_obj['update_group'] == True:
                p = Process(target=create_group, args=(es_query, state['cid'],query_obj['groupid'], True,))
                p.start()
            else:
                p = Process(target=create_group, args=(es_query, state['cid'],query_obj['groupid'],))
                p.start()
        self.write(json.dumps(result))

    def get(self, term):
        self.write('{"code":1004,"msg":"Does not support method GET"}')
