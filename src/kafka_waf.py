# -*- coding: UTF-8 -*-
###spark streaming&&kafka
import string, random
import getopt
import sys,os
CUR_PATH = os.path.split(os.path.realpath(sys.argv[0]))[0]
sys.path.append("%s/../pkg" % CUR_PATH)
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils,TopicAndPartition

##########################################
# define signal when exit
#
import signal
def onsignal_term(a,b):
        raise SystemExit('signal.SIGTERM exit')

def onsignal_int(a,b):
        raise SystemExit('signal.SIGINT exit')

signal.signal(signal.SIGTERM,onsignal_term)
signal.signal(signal.SIGINT,onsignal_int)
############################################
#
# define SparkContext
#
from pyspark import SparkContext,SparkConf
conf = SparkConf().setAppName("Kafka-Streaming WAF")
#
conf.set("spark.speculation","true")
conf.set("spark.speculation.interval",100)
conf.set("spark.speculation.quantile",0.75)
conf.set("spark.speculation.multiplier",1.5)
#
conf.set("spark.yarn.max.executor.failures",10)
#
conf.set("spark.yarn.executor.memoryOverhead",1024)
conf.set("spark.yarn.driver.memoryOverhead",1024)
#
conf.set("spark.storage.memoryFraction",0.3)
conf.set("spark.shuffle.memoryFraction",0.5)
#
#conf.set("spark.scheduler.mode","FAIR")
sc = SparkContext(conf=conf)

#import MySQLdb
#
# import the custom module
#
from domysql import *
from docommand import *
from MySqlConn import Mysql
from doDebug import doSetDebug

WAFCHECKPOINT = 1

if WAFCHECKPOINT:
        checkpointDirectory = "/home/waf"
        checkpointInterval = 120
else:
        checkpointDirectory = ""
        checkpointInterval = 0
        
partitionNum = int(wafPartition)
topicList = [wafTopic]

debugSign = "WAF"

#
# handle attack type
#
def newhandleAttackType(obj):
        attackDict = {"attack-disclosure":"disclosure", \
                      "attack-dos":"ddos", \
                      "attack-reputation-ip":"reputation_ip", \
                      "attack-lfi":"lfi", \
                      "attack-sqli":"sqli", \
                      "attack-xss":"xss", \
                      "attack-injection-php":"injection_php", \
                      "attack-generic":"generic", \
                      "attack-rce":"rce", \
                      "attack-protocol":"protocol", \
                      "attack-rfi":"rfi", \
                      "attack-fixation":"fixation", \
                      "attack-reputation-scanner":"scaning", \
                      "attack-reputation-scripting":"scaning", \
                      "attack-reputation-crawler":"scaning"}
        
        attack = attackDict.get(obj, 'other')
        return attack
#
# change the xdr to one layer structure
# {"A":{"B":"b"}} --> {"A_B":"b"}
#
def getNewWafXdrDict(obj):
        if isinstance(obj, dict) and len(obj) > 0:
                #newXdr = getOneRowXdr(obj)
                doSetDebug(debugSign,"info","getNewWafXdrDict",str(obj))
                newXdr = getNewXdr(obj)
                newXdr['Alert'] = obj['Alert']
                return newXdr
        else:
                return ''
#
# filter xdr and get the one layer structure
#
def changeWafXdrDict(obj):
        doSetDebug(debugSign,"info","changeWafXdrDict","start changeWafXdrDict")
        rdd = checkXdrDict(obj)
        newRDD = rdd.map(lambda x:getNewXdrDict(x)).filter(lambda x:delNullRDD(x))
        return newRDD
#
# (time,disclosure,ddos,reputation_ip,lfi,sqli,\
# xss,injection_php,generic,rce,protocol,\
# rfi,fixation,reputation_scanner,reputation_scripting,reputation_crawler,\
# other)
#
def changeWafCountMysql(connMysql, time, alerttype, num):
        nameList = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        alertJson = {"disclosure": 1, "ddos":2, "reputation_ip":3, \
                     "lfi":4, "sqli":5, "xss":6, \
                     "injection_php":7, "generic":8, "rce":9, \
                     "protocol":10, "rfi":11, "fixation":12, \
                     "scaning":13,"other":14}

        sqli = "call checkwafcount%s"

        time = timeStampToymd(time)
        nameList[0] = time
        alerttype = alerttype.lower()
        i = alertJson.get(alerttype, 14)
        nameList[i] = num
        nameTuple = tuple(nameList)
        result = connMysql.insertOne(sqli, str(nameTuple))
        return result
#
# insert into alert table
#
def insertWafMysql(connMysql, keystuple, valuestuple, tag):
        newkeys = changeMysqlKeyTupleToStr(keystuple)
        newvalues = changeMysqlValueTupleToStr(valuestuple)

        if tag == "offline":
                sqli = "insert into alert_waf_offline%s values%s"
        elif tag == "rule":
                sqli = "insert into alert_waf_offline_rule%s values%s"
        else:
                sqli = "insert into alert_waf%s values%s"
        result = connMysql.insertOne(sqli,(newkeys, newvalues))
        return result

def doWafMysql(connMysql, wafKeys, wafValues, time, alerttype, xdrKeys, xdrValues, tag):
        #pointname = 'wafPoint'
        #s = string.ascii_lowercase
        #pointname = string.join(random.sample(s,5)).replace(" ","")
        #start_info = "start" + " " + pointname
        #doSetDebug(debugSign,"info","doWafMysql",start_info)
        try:
                connMysql.begin(pointName=pointname)
                alert_id = insertWafMysql(connMysql, wafKeys, wafValues, tag)
                insertXdrMysql(connMysql, xdrKeys, xdrValues, alert_id, tag, "WAF")
                if tag != 'offline' and tag != 'rule':
                        changeWafCountMysql(connMysql, time, alerttype, 1)
        except BaseException,err:
                doSetDebug(debugSign,"error","doWafMysql",str(err))
                waf_info = str(wafKeys) + "---" + str(wafValues)
                xdr_info = str(xdrKeys) + "---" + str(xdrValues)
                doSetDebug(debugSign,"error","doWafMysql",str(waf_info))
                doSetDebug(debugSign,"error","doWafMysql",str(xdr_info))
                connMysql.end(pointName=pointname,option="nocommit")
        #finally:
                #end_info = "end" + " " + pointname
                #doSetDebug(debugSign,"info","doWafMysql",end_info)

#
# handle waf xdr, return (keys) and (values)
#
def handleWafInfo(obj):
        global wafOKKeyList
        xdrInfo = obj['Alert']
        ruleDict = xdrInfo.get('Rule', '')
        del xdrInfo['Rule']

        keyList = []
        valueList = []
        for k,v in xdrInfo.items():
                if k == "Attack":
                        keyList.append(str(k))
                        valueList.append(str(newhandleAttackType(v)))
                elif k == "Tags":
                        keyList.append(str(k))
                        if v == None or len(v) == 0:
                                value = ""
                        else:
                                value = reduce(lambda x,y:str(x) + str("_") + str(y), v)
                        valueList.append(value)
                elif k in wafOKKeyList:
                        keyList.append(str(k))
                        valueList.append(str(v))

        if isinstance(ruleDict, dict):
                for k,v in ruleDict.items():
                        key = "Rule" + str('_') + k
                        if key in wafOKKeyList:
                                keyList.append(str(key))
                                valueList.append(str(v))
        addKeyList = list()
        addValueList = list()

        wafAddInfoDict = {"conn_scountry":"src_country","conn_sprovince":"src_province","conn_dregioncode":"",\
                   "conn_slat":"src_latitude","conn_slng":"src_longitude","conn_dcountry":"dest_country",\
                   "conn_dprovince":"dest_province","conn_dlat":"dest_latitude","conn_dlng":"dest_longitude",\
                   "conn_sip":"src_ip","conn_dip":"dest_ip","conn_sport":"src_port",\
                   "conn_dport":"dest_port","conn_proto":"proto","http_request":"request",\
                   "http_response":"response"}
        wafAddInfoList = ["conn_scountry","conn_sprovince","conn_dregioncode","conn_slat","conn_slng",\
                    "conn_dcountry","conn_dprovince","conn_dregioncode","conn_dlat","conn_dlng",\
                    "conn_sip","conn_dip","conn_sport","conn_dport","conn_proto",\
                    "http_request","http_response"]
        for name in wafAddInfoList:
                v = obj.get(name,"")
                #info = str(name) + "=" + str(v)
                #doSetDebug(debugSign,"info","handleWafInfo",str(info))
                k = wafAddInfoDict.get(name)
                if v != "" and k != "":
                        addKeyList.append(k)
                        addValueList.append(str(v))

        keyList.extend(addKeyList)
        valueList.extend(addValueList)

        time = obj.get("time")
        keyList.append("time")
        valueList.append(time)
        tag = obj.get('offline_tag', 'online')
        if tag == "offline" or tag == "rule":
                task_id = obj.get("task_id",0)
                keyList.append('task_id')
                valueList.append(task_id)

        alerttype = newhandleAttackType(xdrInfo.get('Attack','other'))
        return tuple(keyList), tuple(valueList), time, alerttype
#
# handle xdr info return (keys) and (values)
#
def handleXdrInfo(obj):
        #global xdrOKKeyList
        keyList = []
        valueList =[]

        tag = obj.get('offline_tag', 'online')
        #if tag != "offline" and tag != "rule":
        try:
                del obj['offline_tag']
        except KeyError,e:
                e = 1
        try:
                del obj['task_id'] 
        except KeyError,e:
                e = 1
        for k,v in obj.items():
                #if k in xdrOKKeyList:
                keyList.append(k)
                valueList.append(v)

        return tuple(keyList), tuple(valueList), tag

def doWafPartition(iterList):
        s = string.ascii_lowercase
        sss = string.join(random.sample(s,5)).replace(" ","")
        sinfo = "start doWafPartition " + sss
        einfo = "finally " + sss
        doSetDebug(debugSign,"info","doWafPartition",sinfo)
        connMysql = Mysql()
        try:
                for iters in iterList:
                        wafKeys, wafValues, time, alerttype = handleWafInfo(iters)
                        del iters['Alert']
                        xdrKeys, xdrValues, tag = handleXdrInfo(iters)
                        if len(wafKeys) == len(wafValues) and len(wafKeys) > 0:
                                doWafMysql(connMysql,wafKeys,wafValues,time,\
                                           alerttype,xdrKeys,xdrValues,tag)
        except BaseException,err:
                doSetDebug(debugSign,"error","doWafPartition",str(err))
        finally:
		connMysql.dispose()
		doSetDebug(debugSign,"info","doWafPartition",einfo)
                
def rddToJson(rdd):
        dataList = rdd.collect()
        print dataList
#
# get offset from mysql and kafka
#
def getKafkaFromOffset():
        names = locals()
        fromOffset = {}
        connMysql = Mysql()
        
        for top in topicList:
                for partition in range(partitionNum):
                        key = "offset" + top + str(partition)
                        try:
                                names[key]
                        except BaseException:
                                doSetDebug(debugSign,"warning","getKafkaFromOffset","get offset failed .")
                                names[key] = initOffset(connMysql,"waf",top,partition)

                        topicPartion = TopicAndPartition(top, partition)
                        fromOffset[topicPartion] = long(names["offset%s%d" % (top,partition)])

        connMysql.dispose()
        return fromOffset
#
# start accept the values according to the streaming 
#
def startStreaming(obj,sign):
       	#处理时间间隔为5s
       	ssc=StreamingContext(sc, 5)

       	if WAFCHECKPOINT:
               	ssc.checkpoint(checkpointDirectory)

       	if sign == "new":
                doSetDebug(debugSign,"warning","createDirectStream","start from mysql offset failed.")
                doSetDebug(debugSign,"warning","createDirectStream","start from new kafka offset.")
                lines = KafkaUtils.createDirectStream(ssc, \
                                                      topicList, \
                                                      kafkaParams={"metadata.broker.list": brokers})
       	else:
                fromOffset = getKafkaFromOffset()
                doSetDebug(debugSign,"info","startStreaming",str(fromOffset))
                doSetDebug(debugSign,"info","startStreaming","start from mysql offset.")
                lines = KafkaUtils.createDirectStream(ssc, \
                                                      topicList, \
                                                      kafkaParams={"metadata.broker.list": brokers}, \
                                                      fromOffsets=fromOffset)

       	lines1=lines.map(lambda x:x[1])
       	words=lines1.flatMap(lambda line:line.split("\n"))
       	#words.pprint()
       	#streamRDD->RDD->list[json]
       	#words.foreachRDD(rddToJson)

        wafDstream = words.transform(changeWafXdrDict)
        #wafDstream.foreachRDD(rddToJson)
        wafDstream.foreachRDD(lambda rdd:rdd.foreachPartition(doWafPartition))

        def getKafkaOffset(rdd):
                #if not rdd.isEmpty():
                try:
                        offsetRange = rdd.offsetRanges()
                        for o in offsetRange:
                                key = "offset" + o.topic + str(o.partition)
                                obj[key] = o.untilOffset
                except BaseException,err:
                        doSetDebug(debugSign,"error","offsetRanges",str(err))
        lines.foreachRDD(getKafkaOffset)

        if WAFCHECKPOINT:
                lines.checkpoint(checkpointInterval)
        return ssc

def checkInput(argv):
        global WAFCHECKPOINT
        try:
                opts, args = getopt.getopt(argv[1:], 'hc:', ['checkpoint='])
        except getopt.GetoptError, err:
                Usage()
                doSetDebug(debugSign,"error","checkInput",str(err))
                sys.exit(2)
                
        for o, a in opts:
                if o in ('-h', '--help'):
                        Usage()
                        sys.exit(0)
                elif o in ('-c', '--checkpoint'):
                        if a == "yes":
                                WAFCHECKPOINT = 1
                        elif a == "no":
                                WAFCHECKPOINT = 0
                        else:
                                doSetDebug(debugSign,"error","checkInput","input error")
                                Usage()
                                sys.exit(2)


def startOffset(obj):
        getInitOffset()
        try:
                ssc = startStreaming(obj, "command")
                ssc.start()
                ssc.awaitTermination()
        except BaseException, err:
                if "OffsetOutOfRangeException" in str(err):
                        ssc.stop(stopSparkContext=False, stopGraceFully=True)
                        doSetDebug(debugSign,"info","startOffset",str(err))
                        ssc = startStreaming(obj, "new")
                        ssc.start()
                        ssc.awaitTermination()

def startCheckpoint(obj):
        context = StreamingContext.getOrCreate(checkpointDirectory, \
                                                           lambda:startStreaming(obj))
        context.start()
        context.awaitTermination()
        
if __name__ == "__main__":
        checkInput(sys.argv)
        
        newOffset = dict()
        err = delCheckPoint(checkpointDirectory)
        if err:
                doSetDebug(debugSign,"error","main","delete checkpoint directory failed!")
                sys.exit(2)
        try:
                startOffset(newOffset)
        except BaseException,e:
                doSetDebug(debugSign,"error","main",str(e))
        finally:
                connMysql = Mysql()
                for top in topicList:
                        for partition in range(partitionNum):
                                mysqlName = "waf" + top + str(partition)
                                key = "offset" + top + str(partition)
                                offset = newOffset.get(key,"")
                                info = mysqlName + "---" + str(offset)
                                if offset:
                                        updateOffsetMysql(connMysql, mysqlName, offset)

                connMysql.dispose()


