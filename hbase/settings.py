#-*-coding:utf-8-*-
import os
import ConfigParser

# All ADDRESS
#ZK_ADDRESS = '192.168.41.103:2181,192.168.41.104:2181,192.168.41.105:2181'
ZK_ADDRESS = '192.168.50.11:2181,192.168.50.12:2181,192.168.50.13:2181,192.168.50.14:2181,192.168.50.15:2181'
# Hbase
#HBASE_THRIFT_ADDR = '192.168.41.114:9090'
HBASE_THRIFT_ADDR = '192.168.96.253:8080'
# Redis
# sentinel address
SENTINEL_ADDR_USERACTION = '192.168.41.109:26379,192.168.41.110:26379'
SENTINEL_ADDR_ITEM = '192.168.41.109:26379,192.168.41.110:26379'
SENTINEL_ADDR_USERPROFILE = '192.168.41.109:26379,192.168.41.110:26379'

TRIPOD_ADDRESS = '/'.join([ZK_ADDRESS,'Tripod'])
PORT = 9991

PROJECT_PATH = os.path.abspath(os.path.dirname(__file__))

# Default Method
DEFAULT_METHOD=['Visit', 'Order', 'Pay', 'AddCart', 'FeedBack', 'VisitCat', 'Search']
DEFAULT_HTABLE='CleanOfflineUserProfileTempV1'

# Server State 
ARGUMENT_ERROR = 1003
RESULT_EMPTY_ERROR = 1004
SERVER_OK = 0
INTERNAL_ERROR = 1005
METHOD_NOT_SUPPORT = 1006

#设置消息队列的大小
QUEUEMAXSIZE = 100
#配置前缀
CONFIGPREFIX = '/opt/bre/usrTag/bfdopen/bfdopen'
#百分点电商类目体系
BFDCATEFILE = CONFIGPREFIX + '/' +  'config/bfd_cate.dat'
#百分点媒体类目体系
MEDIACATEFILE = CONFIGPREFIX  + '/' + 'config/media_cate.dat'
#定义价格区间
PRICEFILE = CONFIGPREFIX + '/' + 'config/category_price.dat'
#定义JSON映射关系
JSONMAPPING = CONFIGPREFIX + '/' + 'config/json_mapping.dat'
#定义统计指标的映射关系
DIMENSIONMAPPING = CONFIGPREFIX  + '/' + 'config/dmp_config.ini'
#定义城市等级映射关系
CITYLEVELMAPPING = CONFIGPREFIX + '/' + 'config/city_level.dat'

#定义城市别名
CITYALIAS = CONFIGPREFIX + '/' + 'config/city_alias.dat'

#定义LabelID指标的映射关系
LABELMAPPING = CONFIGPREFIX  + '/' + 'config/labelID.dat'

#定义AttributeInfo指标的映射关系
ATTRIMAPPING = CONFIGPREFIX + '/' + 'config/attributeInfo.dat'




# 省会映射
PROVINCE_MAP={"台湾":"台湾","香港":"香港","上海市":"上海","云南省":"云南","内蒙古自治区":"内蒙古","北京市":"北京","台湾":"台湾","吉林省":"吉林","四川省":"四川","天津市":"天津","宁夏回族自治区":"宁夏","安徽省":"安徽","山东省":"山东","山西省":"山西","广东省":"广东","广西壮族自治区":"广西","新疆维吾尔自治区":"新疆","江苏省":"江苏","江西省":"江西","河北省":"河北","河南省":"河南","浙江省":"浙江","海南省":"海南","湖北省":"湖北","湖南省":"湖南","澳门":"澳门","甘肃省":"甘肃","福建省":"福建","西藏自治区":"西藏","贵州省":"贵州","辽宁省":"辽宁","重庆市":"重庆","陕西省":"陕西","青海省":"青海","黑龙江省":"黑龙江"}

INDUSTRY_LIST = ['财经','图书', '彩票', 'IT数码', '汽车', '军事','时尚','影视','母婴育儿', '医疗健康','房产','教育培训','商旅出行','游戏','动漫','家装建材', '应用软件', '科技']


#保存百分点标准电商品类体系
bfd_json_mapping = {}

#保存百分点标准电商品类体系
bfd_categorys = {}

#保存百分点标准媒体品类体系
bfd_medias = {}

#保存百分点品类价格区间
price_level = {}

cf = ConfigParser.ConfigParser()
cf.read(DIMENSIONMAPPING)

#人口统计学维度指标映射关系
dg_itemset = cf.items("dg_info")
#上网特征维度指标映射关系
internet_itemset = cf.items("internet_info")
#营销特征维度指标映射关系
market_itemset = cf.items("market_info")

#城市别名映射表
city_alias_dic = {}
with open(CITYALIAS, 'r') as f:
    for line in f:
        items = line.strip().split(',')
        if len(items) != 2:
            continue
        city_alias_dic.update({items[1]:items[0]})

#城市类型离散化映射关系
type_level_dic = {1:"一线城市", 2:"二线城市", 3:"三线城市", 4:"四线城市", 5:"五线城市"} 

#城市类型映射表
city_level_dic = {}
with open(CITYLEVELMAPPING, 'r') as f:
    for line in f:
        items = line.strip().split('\t')
        if (len(items) != 2) and (int(items[1]) not in type_level_dic):
            continue
        city_level_dic.update({items[0]:type_level_dic[int(items[1])]})

#for city in city_level_dic:
#    print "%s:%s" %(city, city_level_dic[city])    

#labelID映射表
labelID_dic = {}
#百分点LabelID映射表
with open(LABELMAPPING) as f:
    for line in f:
        aList = line.strip('\n').split(',')
        if len(aList) != 2:
            print "length error line: %s" %(line)
            continue
        key = aList[0]
        if key in labelID_dic:
            print "repeat error line: %s" %(line)
            continue
        labelID_dic[key] = aList[1]

#AttributeInfo映射表
attr_dic = {}
with open(ATTRIMAPPING, 'r') as f:
    for line in f:
        aList = line.strip().split('\t')
        if len(aList) != 3:
            print "length error line: %s" %(line)
            continue
        type = aList[0]
        if type not in attr_dic:
            attr_dic.update({type:{}}) 
        key = aList[1]
        value = aList[2]
        if key in attr_dic[type]:
            print "Duplicate key in attr_dic: %s" %(line)
            continue
        attr_dic[type].update({key:value})  

#加载百分点JSON属性映射体系
with open(JSONMAPPING, 'r') as f:
    for line in f:
        array = line.rstrip('\n').split()
        if len(array) != 2:
            continue
        key = array[0]
        mapping_name = array[1]
        if key not in bfd_json_mapping:
            bfd_json_mapping.update({key:mapping_name})
        else:
            print "The %s dose exits in bfd_json_mapping. " %s(key)

#加载百分点电商标准类目体系
with open(BFDCATEFILE, 'r') as f:
    for line in f:
        array = line.rstrip('\n').split()
        if len(array) != 2:
            continue
        first_category = array[0]
        second_category = array[1]
        if first_category not in bfd_categorys:
            bfd_categorys.update({first_category:set()})
        bfd_categorys[first_category].add(second_category)

# 加载百分点媒体标准类目体系
with open(MEDIACATEFILE, 'r') as f:
    for line in f:
        array = line.rstrip().split()
        if len(array) < 2:
            continue
        first_category = array[0]
        second_category = array[1]
        if first_category not in bfd_medias:
            bfd_medias.update({first_category:set()})
        bfd_medias[first_category].add(second_category)


#百分点标准电商品类价格区间
count_map = {}
with open(PRICEFILE, 'r') as f:
    for line in f:
        itemset = line.strip().split(',')
        if len(itemset) < 2:
            print "Error: the length of line is less than 2. " + line
            continue
        else:
            category = itemset[0]
            if category not in price_level:
                price_level[category] = {}
            if category not in count_map:
                count_map.update({category:0})
            count = count_map[category]
            count += 1
            count_map[category] = count
            if len(itemset) > 2:
                price_level[category].update({str(count):[itemset[1], itemset[2]]})
            else:
                price_level[category].update({str(count):[itemset[1]]})

#年龄映射表(age)
ageMap={'Teenage':'18岁以下', 'Youngster':'18-24岁','Young':'25-34岁','Mid-Age':'35-49岁','Elder':'49岁以上'}

#消费等级映射表(price_level)
priceLevelMap={1:'等级1',2:'等级2',3:'等级3',4:'等级4', 5:'等级5', 6:"等级6", 7:"等级7", 8:"等级8", 9:"等级9"}

#上网时段映射表(internet_time)
intervalMap={"1":"0点 至 7点","2":"7点 至 11点","3":"11点 至 15点","4":"15点 至 19点","5":"19点 至 22点","6":"22点 至 24点"}

#上网时长映射表(online_time)
onlineTimeMap={"1":"不足1小时","2":"1-4小时","3":"4-8小时","4":"8小时以上"}

#上网频次映射表(frequency)
freqMap={"1":"1-2次","2":"3-4次","3":"5-7次"}

#终端分布映射表(terminal)
termTypeMap = {"pc":"PC", "mobile":"手机", "tablet":"平板"}


dg_info_dic = {"dg_info.inter_sex":"互联网/性别","dg_info.inter_age":"互联网/年龄段","dg_info.inter_has_baby":"互联网/是否有子女","dg_info.inter_marriage":"互联网/婚姻状况","dg_info.natural_age":"自然属性/年龄段","dg_info.natural_sex":"自然属性/性别","dg_info.region_provice":"地区地域/省","dg_info.region_city":"地区地域/市"}

inter_pc_Info_dic = {"inter_ft.oper_systems" : "操作系统", "inter_ft.browser" : "浏览器", "inter_ft.online_time" : "上网时长", "inter_ft.frequency" : "上网频次", "inter_ft.terminal_types" : "终端类型","inter_ft.chanel_PC" : "上网渠道PC","inter_ft.internet_time":"上网时段","inter_ft.terminal_brands":"终端品牌"}

inter_mobile_Info_dic = {"inter_ft.oper_systems":"操作系统","inter_ft.frequency":"上网频次","inter_ft.chanel_mobile":"上网渠道移动端","inter_ft.terminal_brands":"终端品牌","inter_ft.terminal_types":"终端类型","inter_ft.online_time":"上网时长","inter_ft.browser":"浏览器","inter_ft.access_way":"上网方式","inter_ft.internet_time":"上网时段"}

market_ft_dic = {"market_ft.con_money":"营销/消费金额","market_ft.industry_name":"营销/行业名称", "market_ft.con_capacity_new":"营销/消费能力", "market_ft.con_level_new":"营销/消费层级", "market_ft.con_period_new":"营销/消费周期", "market_ft.price_sensitivity_new":"营销/价格敏感度"}


#分类映射信息
type_dic = {1:"长期购物偏好", 2: "长期兴趣偏好"}

query_res = {"date":"2015-09-23","total":167812659,"data":[{"value":{"已婚":220454},"labelID":"000010000100003"},{"value":{"有孩子":209137},"labelID":"000010000100006"},{"value":{"安徽":4208298,"山西":3648321,"黑龙江":2999256,"江苏":10366715,"湖南":4298359,"江西":3049439,"北京":10462568,"四川":5940684,"广西":3634207,"湖北":4831442,"福建":5133223,"广东":19050267,"辽宁":4344542,"河北":6636284,"上海":6751110,"陕西":3970200,"浙江":8630557,"河南":9349531,"云南":2247330,"山东":9777646},"labelID":"000010000300001"},{"value":{"25-34岁":6784897,"35-49岁":12407520,"18-24岁":115335,"18岁以下":4},"labelID":"000010000100002"},{"value":{"18-24岁":2121589,"25-34岁":24291986,"35-49岁":16174762,"49岁以上":3692643,"18岁以下":444367},"labelID":"000010000200002"},{"value":{"女":27769457,"男":18425215},"labelID":"000010000200001"},{"value":{"女":8980242,"男":5182287},"labelID":"000010000100001"},{"value":{"福州市":1431221,"广州市":4568724,"济南市":1583578,"南京市":2722317,"长沙市":1507241,"武汉市":2463000,"上海市":6751110,"天津市":2457178,"重庆市":2553945,"郑州市":3858391,"成都市":2787725,"石家庄市":1801406,"合肥市":1467438,"苏州市":2234036,"西安市":2513811,"北京市":10462568,"杭州市":2372829,"东莞市":1854230,"深圳市":4860302,"佛山市":1879200},"labelID":"000010000300002"},{"value":{"IE9":5417467,"IE8":15963669,"Chrome42":3718878,"Chrome":3702639,"IE7":8439400,"IE6":4002265,"IE11":5373954,"搜狗浏览器":4379840,"猎豹浏览器":2843097,"360安全浏览器":25511003},"labelID":"000020000100007"},{"value":{"8小时以上":5807,"4-8小时":2557650,"不足1小时":1783962,"1-3小时":19984122},"labelID":"000020000100002"},{"value":{"3-4天":3083949,"1-2天":48995957,"5-7天":992229},"labelID":"000020000100003"},{"value":{"14:00:00 至 16:59:59":20048838,"12:00:00 至 13:59:59":8956143,"19:00:00 至 21:59:59":18527474,"17:00:00 至 18:59:59":9837796,"22:00:00 至 24:59:59":10448861,"7:00:00 至 8:59:59":3948841,"9:00:00 至 11:59:59":16856585,"1:00:00 至 6:59:59":5285437},"labelID":"000020000100001"},{"value":{"Android 5.0":6286030,"Windows 8.1":6813712,"Windows XP":67991370,"Android 4.4":36101234,"Android 4.1":6336943,"iOS 8.3":6441861,"Android 4.2":12085735,"Android 4.3":9341093,"Windows 7":101734093,"iOS 8.1":7365729},"labelID":"000020000100004"},{"value":{"Chrome移动版39":1268521,"UC浏览器10":3588639,"Chrome移动版30":1647856,"QQ手机浏览器5.4":3261635,"Safari移动版8.0":4230221,"Chrome移动版33":2438321,"Android Webkit Browser4.0":12900248,"Safari移动版7.0":1249758,"QQ手机浏览器6.1":2456326,"UC浏览器10.6":2597699},"labelID":"000020000200008"},{"value":{"8小时以上":4048,"4-8小时":956494,"不足1小时":1052971,"1-3小时":6833447},"labelID":"000020000200002"},{"value":{"3-4天":1181130,"1-2天":28675164,"5-7天":451592},"labelID":"000020000200003"},{"value":{"14:00:00 至 16:59:59":9535651,"12:00:00 至 13:59:59":5588791,"19:00:00 至 21:59:59":12441713,"17:00:00 至 18:59:59":6034692,"22:00:00 至 24:59:59":11302454,"7:00:00 至 8:59:59":3890848,"9:00:00 至 11:59:59":7905610,"1:00:00 至 6:59:59":5283003},"labelID":"000020000200001"},{"value":{"Android 5.0":6286030,"Windows 8.1":6813712,"Windows XP":67991370,"Android 4.4":36101234,"Android 4.1":6336943,"iOS 8.3":6441861,"Android 4.2":12085735,"Android 4.3":9341093,"Windows 7":101734093,"iOS 8.1":7365729},"labelID":"000020000200004"},{"value":{"4G":87943,"2G":575100,"WIFI":4372120,"3G":434959},"labelID":"000020000200007"}]}
