# coding=utf-8
import urllib, urllib2, json
import hashlib
import requests
from settings import *
from views_phone import BRData
import Queue
import threading
import os
import commands
import log_util
from paths import *


brData = {}
class ThreadUrl(threading.Thread):
    """Threaded Url Grab"""
    def __init__(self, queue, brInst, data_url, tid, task_id, is_md5):
        threading.Thread.__init__(self)
        self.queue = queue
        self.brInst = brInst
        self.data_url = data_url
        self.tid = tid
        self.task_id = task_id
        self.is_md5 = is_md5

          
    def run(self):
        while True:
            cell = self.queue.get()
            #print "before: " + cell
            try:
                result = self.brInst.getData(url = self.data_url, param = 'cell', value = [cell], tid = self.tid, is_md5=self.is_md5)
                #if result['code'].strip() == '00':
                    #print result
                #print
                print result
                brData.update({cell:result})
            except Exception,e :
                pass
                #print e
                #print "Error cell: " + cell
            #print "After: " + cell
            log_util.add_log(task_id, '/opt/bre/100credit/log/')
            self.queue.task_done()

def pull_data(file_name, task_id, output_dir, data_format):
    userName = 'baifendian_3'
    passWd = 'baifendian_3'
    apiCode = '100041'
    if data_format == 'md5':
        userName = 'baifendian'
        passWd = 'baifendian'
        apiCode = '100025'

    print 'username',userName
    print 'passwd', passWd
    print 'apicode', apiCode
    login_url = "https://api.100credit.cn/bankServer2/user/login.action"
    print '11111111111'
    brInst = BRData(userName = userName, passWd = passWd, apiCode = apiCode)
    tid = brInst.login(url = login_url)
    print '22222'
    data_url = "https://api.100credit.cn/bankServer2/data/bankData.action"
    month_list = ["month3", "month6", "month12"]
    business_month_dic = {}
    media_month_dic = {}
    upAttr_list = ['phone,city,lastweekdays,totaldays,lasttime,opsys,sitetype,lastweekdays,totaldays,lasttime,brcreditpoint,title,house,car,fin,wealth,top1,top2,top3,top4,top5']
    for month in month_list:
        business_month_dic.update({month:['phone,类别,visits,number,pay,maxpay']})
        media_month_dic.update({month:['phone,类别,visitdays']})

    if data_format == 'md5':
        is_md5 = True
    else:
        is_md5 = False
    
    task_folder = BR_TASK_PATH
    log_folder = BR_LOG_PATH
    reader = open(task_folder+file_name,'r')
    n_records = len(reader.readlines())
    print "fils_totline: ",n_records
    reader.close()
    log_util.create_task((task_id,n_records),log_folder)

    os.environ["dir"] = BR_TASK_PATH+"br/"
    os.environ['filename']=str(file_name)
    os.environ["fid"] = BR_TASK_PATH+str(file_name)

    if os.path.exists(BR_TASK_PATH+"br/"):
        os.system("rm -rf $dir")
    try:
        os.system("mkdir $dir; cp $fid $dir; cd $dir; split -b 20m $fid br_data; rm -rf $filename")
        files = commands.getoutput("cd $dir;ls -l|awk '{print $9}'").rstrip('\n').split('\n')
        #print "files: ",files
    except Exception,e :
        print e
    
    for j in range(len(files)):
        if j == 0:
            continue
        queue = Queue.Queue()
        for i in range(10):
            t = ThreadUrl(queue, brInst, data_url, tid, task_id, is_md5)
            t.setDaemon(True)
            t.start()
        file_name = files[j]
        #reader = open(task_folder+file_name, 'r') 
        #n_records = len(reader.readlines())  # 计数记录
        #reader.close()
        #log_util.create_task((task_id,n_records), log_folder)
        with open(BR_TASK_PATH+"br/"+file_name, 'r') as f:
            for i,line in enumerate(f):
                cell = line.strip()
                queue.put(cell)
        queue.join()
        for cell in brData:
            result = brData[cell]
            for month in month_list:
                try:
                    #电商行为
                    result_month = result["Consumption"][month]
                    #print result_month
                    brInst.getCategory(cell = cell, result = result_month,mapping = br_category_mapping,brList = brConList, result_list = business_month_dic.get(month))
                    #print business_month_dic.get(month)
                    #print
                except Exception, e:
                    pass
                    #print str(e)
                try:
                    #媒体行为
                    result_month = result["Media"][month]
                    brInst.getCategory(cell = cell, result = result_month, mapping = br_category_mapping,brList =  brMediaList, result_list = media_month_dic.get(month))
                except Exception, e:
                    pass
                    #print str(e)
                     
            try:
                #用户画像标签
                brInst.getUpAttributes(cell = cell, result = result, brDic = brUpAttrsDic,brList = brUpAttrsList, result_list = upAttr_list)
            except Exception, e:
                pass
                #print str(e)    

        #print business_month_dic
        for key in business_month_dic:
            brInst.save2file(results = business_month_dic[key], fout_path = output_dir + "Business_" + key + ".txt")
        for key in media_month_dic:
            brInst.save2file(results = media_month_dic[key], fout_path = output_dir + "Media_" + key + ".txt")
        
        brInst.save2file(results = upAttr_list, fout_path = output_dir + "up.txt")
        # clear_global_variable
        brData.clear()
        business_month_dic.clear()
        media_month_dic.clear()
        #for month in month_list:
        #    business_month_dic.update({month:['phone,类别,visits,number,pay,maxpay']})
        #    media_month_dic.update({month:['phone,类别,visitdays']})
        upAttr_list =[]
        #upAttr_list = ['phone,city,lastweekdays,totaldays,lasttime,opsys,sitetype,lastweekdays,totaldays,lasttime,brcreditpoint,title,house,car,fin,wealth,top1,top2,top3,top4,top5'] 

    #os.system("rm -rf $dir")
    print task_id, "finish"

    print 'output dir', output_dir
    os.system('sh extract_up_br.sh '+ output_dir+' '+' userprofile.xls')
    os.system('sh br_upload_ftp.sh ' + output_dir)


if __name__ == "__main__":
    import sys
    file_name = sys.argv[1]
    task_id = sys.argv[2]
    output_dir = sys.argv[3]
    data_format = sys.argv[4]
    pull_data(file_name, task_id, output_dir, data_format)
    
