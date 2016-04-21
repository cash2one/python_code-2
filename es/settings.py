#-*-coding:utf-8-*-
import sys
import types
reload(sys)
sys.setdefaultencoding('utf-8')
import json
INTERFACE='dmp_test'
#百分点电商类目体系
BFDCATEFILE = '../configure/bfd_cate.dat'
#百分点媒体类目体系
MEDIACATEFILE = '../configure/media_cate.dat'
#定义价格区间
PRICEFILE = '../configure/category_price.dat'
#百分点标准电商品类白名单
TAGWHITEFILE = '../configure/tag_white.dat'
#百分点标准电商品类黑名单
TAGBLACKFILE = '../configure/tag_black.dat'
#百分点标准电商品类品牌黑名单
BRANDBLACKFILE = '../configure/brand_black.dat'
#热搜索词黑名单
OSBLACKFILE = '../configure/os_black.dat'
#操作系统属性黑名单
SEARCHBLACKFILE = '../configure/search_black.dat'
#读书行业热词名单 
BOOKHWORD= '../configure/book_hword.dat'
#品牌归一化映射名单
BRANDMAPPING = '../configure/brand_mapping.dat'
#测试数据
def printMap(map):
  for key in map:
    for subkey in map[key]:
      if (type(map[key][subkey]) == type([])) or (type(map[key][subkey]) == type(())):
        print  key + ":" + ','.join(map[key][subkey])
      else:
        print key + ":" + map[key][subkey]
      
    

# 城市映射
PROVINCE_MAP={"台湾":"台湾","香港":"香港","上海市":"上海","云南省":"云南","内蒙古自治区":"内蒙古","北京市":"北京","台湾":"台湾","吉林省":"吉林","四川省":"四川","天津市":"天津","宁夏回族自治区":"宁夏","安徽省":"安徽","山东省":"山东","山西省":"山西","广东省":"广东","广西壮族自治区":"广西","新疆维吾尔自治区":"新疆","江苏省":"江苏","江西省":"江西","河北省":"河北","河南省":"河南","浙江省":"浙江","海南省":"海南","湖北省":"湖北","湖南省":"湖南","澳门":"澳门","甘肃省":"甘肃","福建省":"福建","西藏自治区":"西藏","贵州省":"贵州","辽宁省":"辽宁","重庆市":"重庆","陕西省":"陕西","青海省":"青海","黑龙江省":"黑龙江"}

INDUSTRY_LIST = ['财经','图书', '彩票', 'IT数码', '汽车', '军事','时尚','影视','母婴育儿', '医疗健康',\
    '房产','教育培训','商旅出行','游戏','动漫','家装建材', '应用软件', '科技']

CLIENT_CONF = {
    'Cwbiao':{
        'INDUSTRY':'钟表',
        'FIRST_CATEGORYS':['浪琴Longines', '欧米茄Omega', '天梭Tissot', '美度MIDO', '百达翡丽Patek Philippe', '卡西欧Casio', '劳力士Rolex', '西铁城Citizen', '帝舵Tudor', '赫柏林Michel Herbelin', '万国IWC', '卡地亚Cartier', '爱宝时EPOS', '梅花Titoni', '迪沃斯DAVOSA', '阿玛尼Armani','积家Jaeger-LeCoultre', 'CKCalvin Klein', '汉米尔顿Hamilton', '格拉苏蒂·莫勒Muehle-Glashuette', '泰格豪雅TAG Heuer', '精工SEIKO', '库尔沃Cuervo y Sobrinos', '宝玑Breguet', '伯爵Piaget', 'GuessGuess'],
        'COMBINATION':{},
        'ANOTHER':{'泰格豪雅TAG Heuer':'泰格豪雅','浪琴Longines':'浪琴', '欧米茄Omega':'欧米茄','天梭Tissot':'天梭', '美度MIDO':'美度', '百达翡丽Patek Philippe':'百达翡丽', '卡西欧Casio':'卡西欧', '劳力士Rolex':'劳力士', '西铁城Citizen':'西铁城', '帝舵Tudor':'帝舵', '赫柏林Michel Herbelin':'赫柏林', '万国IWC':'万国','卡地亚Cartier':'卡地亚', '爱宝时EPOS':'爱宝时', '梅花Titoni':'梅花', '迪沃斯DAVOSA':'迪沃斯', '阿玛尼Armani':'阿玛尼', '积家Jaeger-LeCoultre':'积家', 'CKCalvin Klein':'CK','汉米尔顿Hamilton':'汉米尔顿','精工SEIKO':'精工', '库尔沃Cuervo y Sobrinos':'库尔沃', '宝玑Breguet':'宝玑','伯爵Piaget':'伯爵','GuessGuess':'Guess','格拉苏蒂·莫勒Muehle-Glashuette':'格拉苏蒂·莫勒'},
        'SECOND_COMBINATION':{}
    },
    'Czgjs':{
        'INDUSTRY':'新闻',
        'FIRST_CATEGORYS':['新闻', '江苏新闻','游戏','娱乐','江苏','文化','健康','生活','旅游','中国江苏','教育','财经','汽车','体育','舆情','军事','评论','政风热线','数码家电','组工在线','图片','江苏网江'],
        'COMBINATION':{},
        'ANOTHER':{},
        'SECOND_COMBINATION':{}
    },
    'Cdianyingwang':{
        'INDUSTRY':'影视',
        'FIRST_CATEGORYS':['大片', '内地', '港片', '独家', '微电影', '明星星闻', '欧美', '电影新闻', '综艺', '其他', '纪录片', '韩片', '专访', '八卦', '明星', '网络','光影星播客', '招商频道', '资讯快车', '首映·典礼'],
        'COMBINATION':{'首映·典礼':['首映 2']},
        'ANOTHER':{},
        'SECOND_COMBINATION':{}
    },
    'C5cn':{
        'INDUSTRY':'读书',
        'FIRST_CATEGORYS':['情感', '风景', '人物', '其它', '时尚', '教育', '娱乐', '艺术', '星座', '美图', '养生', '旅游', '文化', '音乐', '历史', '新闻', '漫画', '美食', '两性', '宠物', '房产', '汽车', '科技', '军事', '测试', '家居', '财经', '体育', '游戏'],
        'COMBINATION':{},
        'ANOTHER':{},
        'SECOND_COMBINATION':{}
    },

    'Czhongcai': {
        'INDUSTRY':'彩票',
        'FIRST_CATEGORYS':['彩票种类','新闻','论坛','走势图','双色球',\
            '3D','首页','彩种频道','公益','省市之窗','七乐彩',\
            '视频频道','政策','号码分析','投注擂台','刮刮乐'],
        'COMBINATION':{},
        'ANOTHER':{},
        'SECOND_COMBINATION':{}
    },

    'Chexunwang': {
        'INDUSTRY':'财经',
        'FIRST_CATEGORYS':['股票', '黄金', '科技', '新闻']
    },

    'Ccaixingwang':{
        'INDUSTRY':'财经',
        'FIRST_CATEGORYS':['经济','金融', '公司', '政经', '世界', '观点网', '文化','博客', '周刊', '图片', '视频', '杂志', '财新网首页', '特色', '视听'],
        'COMBINATION':{'观点网':['观点'], '杂志':['杂志频道'], '周刊':['《新世纪》周刊', '《新世纪》周刊频道']},
        'ANOTHER':{},
        'SECOND_COMBINATION':{}
    },

    'Cbili':{
        'INDUSTRY':'影视',
        'FIRST_CATEGORYS':["番剧","连载剧集","生活娱乐","电影","完结剧集","音乐","游戏攻略·解说",\
                           "综合","电子竞技", "鬼畜", "游戏视频", "动画", "舞蹈", "音乐视频",\
                           "综艺", "全球科技", "MMD·3D", "纪录片", "趣味科普", "野生技术协会",\
                           "美食", "完结动画", "VOCALOID·UTAU", "连载动画", "动物圈","游戏",\
                           "Korea相关", "电视剧", "原创·配音", "特摄·布袋"],
        'COMBINATION':{"电影":["電影", ], "连续剧集":["連載劇集"], "番剧":["番劇"], "完结剧集":["完結劇集"],\
                       "生活娱乐":["生活娛樂"], "游戏攻略·解说":["遊戲攻略·解說"],\
                       "全球科技":["科技"], "音乐":["音樂"],"音乐视频":["音樂視頻"],"电视剧":["電視劇"],\
                       "游戏攻略.解说":["游戲攻略·解說"], "动画":["動畫"]},
        'SECOND_COMBINATION':{"連載動畫":"连载动画", "完結動畫":"完结动画", "電影":"电影", "美劇":"美剧",\
                              "日劇":"日剧", "三次元音樂":"三次元音乐", "單機遊戲":"单机游戏",\
                              "網絡遊戲":"网络游戏", "數碼科技":"数码科技","預告‧花絮":"预告·花絮"},
        'ANOTHER':{"VOCALOID·UTAU":"VOCALOID"}
                              
        },
    'C17k':{
        'INDUSTRY':'读书',
        'FIRST_CATEGORYS':['都市小说','玄幻小说', '仙侠小说', '都市言情小说', '游戏小说', '历史小说', '穿越小说','惊悚小说', '军事小说', '同人小说', '科幻小说', '古装言情小说', '幻想言情小说', '奇幻小说', '武侠小说', '耽美小说', '畅销经典小说', '竞技小说'],
        'COMBINATION':{'穿越重生小说':['穿越小说'], '耽美小说':['耽美同人小说']},
        'ANOTHER':{},
        'SECOND_COMBINATION':{}
    },
    'Cxinhua':{
        'INDUSTRY':'金融',
        'FIRST_CATEGORYS':['健康险','女性险','寿险','少儿险','意外险','旅行险','理财险'],
        'COMBINATION':{},
        'ANOTHER':{},
        'SECOND_COMBINATION':{}
    },
    'Czhaoshangyinhang':{
        'INDUSTRY':'金融',
        'FIRST_CATEGORYS':[],
        'COMBINATION':{},
        'ANOTHER':{},
        'SECOND_COMBINATION':{}
    },
    'Ccaikr':{
        'INDUSTRY':'金融',
        'FIRST_CATEGORYS':[],
        'COMBINATION':{},
        'ANOTHER':{},
        'SECOND_COMBINATION':{}
    },
    'Cetongdai':{
        'INDUSTRY':'金融',
        'FIRST_CATEGORYS':[],
        'COMBINATION':{},
        'ANOTHER':{},
        'SECOND_COMBINATION':{}
    }
}



#保存百分点标准电商品类体系
bfd_categorys = {}

#保存百分点标准媒体品类体系
bfd_medias = {}

#保存第一方类目热词黑名单
tag_blacklist = set()

#保存百分点标准电商品类热词白名单
brand_white = {}

#保存百分点标准电商品类品牌黑名单
brand_blacklist = {}

#保存操作系统属性黑名单
os_black = {}

#保存百分点品类价格区间
price_level = {}

#保存百分点品电商品类热搜词黑名单
search_black = {}

#保存百分点品电商品类热搜词黑名单
book_tag = {}

#保存百分点品牌映表
brand_mapping = {}

#保存读书-行业偏好热词
with open(BOOKHWORD, 'r') as f:
  for line in f:
    itemset = line.strip().split(',')
    if len(itemset) != 2:
      print 'Error in ' + BOOKHWORD + ', ' + line
      continue
    if itemset[0] not in book_tag:
      book_tag[itemset[0]] = []
    book_tag[itemset[0]].append(itemset[1])

#加载热搜索词黑名单:search_black
with open(SEARCHBLACKFILE, 'r') as f:
  for line in f:
    itemset = line.strip().split(':')
    if len(itemset) != 2:
      continue
    if itemset[0] not in search_black:
      search_black[itemset[0]] = {}
    search_black[itemset[0]].update({itemset[0]:1})
  

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

# 加载标签黑名单
with open(TAGBLACKFILE, 'r') as f:
    for line in f:
        tag_blacklist.add(line.rstrip('\n'))

# 加载百分点标准电商品类对应品牌的黑名单
with open(BRANDBLACKFILE, 'r') as f:
    for line in f:
        cate = line.rstrip('\n').split(',')[0]
        if not cate in brand_blacklist:
            brand_blacklist.update({cate:set()})
        brand = line.rstrip('\n').split(',')[1]
        brand_blacklist[cate].add(brand)


#操作系统黑名单
with open(OSBLACKFILE, 'r') as f:
  for line in f:
    elems = line.strip().split(':')
    os_black.update({elems[0]:elems[1]})

#添加百分点电商品类品牌白名单
with open(TAGWHITEFILE, 'r') as f:
  for line in f:
    itemset = line.strip().split(',')
    if len(itemset) < 2:
      print "Error: the length of line is less than 2. " + line
      continue
    if itemset[0] not in brand_white:
      brand_white[itemset[0]] = set()
    brand_white[itemset[0]].add(itemset[1])

#添加百分点品牌映射归一化名单
with open(BRANDMAPPING, 'r') as f:
    for line in f:
        itemset = line.strip().split(',')
        if len(itemset) < 2:
            print "Error: the length of is less than 2. " + line
            continue
        brand_mapping.update({itemset[0]: itemset[1]})

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


#年龄映射表
ageMap={'Teenage':'18岁以下', 'Youngster':'18-24岁','Young':'25-34岁','MidAge':'35-49岁','Mid-Age':'35-49岁','Elder':'49岁以上'}
#人生阶段
lifeMap={'求学':'求学期'}

#消费等级
priceLevelMap={1:'等级1',2:'等级2',3:'等级3',4:'等级4', 5:'等级5', 6:"等级6", 7:"等级7", 8:"等级8", 9:"等级9"}
#浏览器
browserList=['MQQBrowser','Chrome', 'Firefox','IE 8', 'Mozilla', 'IE 7', 'Safari', '360se','Sogou', 'QQBrowser', 'IE 9', 'IE 10']
#上网时段
intervalMap={"1":"0:00:00 至 6:59:59","2":"7:00:00 至 10:59:59","3":"11:00:00 至 14:59:59","4":"15:00:00 至 18:59:59","5":"19:00:00 至 21:59:59","6":"22:00:00 至 23:59:59"}
#上网时长
onlineTimeMap={"1":"不足1小时","2":"1-4小时","3":"4-8小时","4":"8小时以上"}
#上网频次
freqMap={"1":"1-2次","2":"3-4次","3":"5-7次"}
#终端分布
termTypeMap = {"pc":"PC","mobile":"手机","tablet":"平板"}
