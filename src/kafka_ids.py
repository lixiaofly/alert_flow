# -*- coding: UTF-8 -*-
###spark streaming&&kafka
import string,random
import json
import getopt
import sys,os
CUR_PATH = os.path.split(os.path.realpath(sys.argv[0]))[0]
sys.path.append("%s/../pkg" % CUR_PATH)
sys.path.append("%s/../conf" % CUR_PATH)
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils,TopicAndPartition
#import MySQLdb
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
###################################
#
# define SparkContext
#
from pyspark import SparkContext,SparkConf
conf = SparkConf().setAppName("Kafka-Streaming IDS")
#change driver
conf.set("spark.speculation","true")
conf.set("spark.speculation.interval",100)
conf.set("spark.speculation.quantile",0.75)
conf.set("spark.speculation.multiplier",1.5)
#
conf.set("spark.yarn.max.executor.failures",10)
#over memery
conf.set("spark.yarn.executor.memoryOverhead",1024)
conf.set("spark.yarn.driver.memoryOverhead",1024)
#
conf.set("spark.storage.memoryFraction",0.3)
conf.set("spark.shuffle.memoryFraction",0.5)
#local
#conf.set("spark.scheduler.mode","FAIR")

sc = SparkContext(conf=conf)
#
# import the custom module
#
from domysql import *
from MySqlConn import Mysql
from docommand import *
from doDebug import doSetDebug


IDSCHECKPOINT = 1
if IDSCHECKPOINT:
        checkpointDirectory = "/home/ids"
        checkpointInterval = 120
else:
        checkpointDirectory = ""
        checkpointInterval = 0
        
partitionNum = int(idsPartition)
topicList = [idsTopic]

debugSign = "IDS"

#
# handle ids byzoro_type 
#
def handleByzoroType(obj):
        byzoroDict = {"Privilege-Gain":"privilege_gain", \
                      "DDos":"ddos", \
                      "Information-Leak":"information_leak", \
                      "Web-Attack":"web_attack", \
                      "Application-Attack":"application_attack", \
                      "CandC":"candc", \
                      "Malware":"malware", \
                      "Misc-Attack":"misc_attack"}
        byzoroType = byzoroDict.get(obj, 'other')
        return byzoroType
#
# json.loads()
#
def checkIdsJson(obj):
        try:
                err = json.loads(obj)
        except ValueError,err:
                doSetDebug(debugSign,"warning","checkIdsJson",str(err))
                err = ''
        finally:
                return err
#
# check ids alert dict
#
def checkIdsDict(rdd):
        newRDD = rdd.map(lambda x:checkIdsJson(x))
        return newRDD
#
# get the new ids dict
#
def getNewIdsDict(obj):
        if isinstance(obj, dict) and len(obj) > 0:
                obj['Time'] = timeWSToS(obj['Time'])
                obj['Proto'] = protocolDict.get(obj['Proto'], 'udp')
                obj['Byzoro_type'] = handleByzoroType(obj['Byzoro_type'])
                return obj
        else:
                return {}
#
# do checkIdsDict() and getNewIdsDict()
#
def changeIdsDict(obj):
        doSetDebug(debugSign,"info","changeIdsDict","start changeIdsDict")
        rdd = checkIdsDict(obj)
        newRDD = rdd.map(lambda x:getNewIdsDict(x)).filter(lambda x:delNullRDD(x))

        return newRDD
#
# (time,privilege_gain,ddos,information_leak,web_attack,application_attack,\
# candc,malware,misc_attack,other)
#
def changeIdsCountMysql(connMysql, time, alertType, num):
        nameList = [0,0,0,0,0,0,0,0,0,0]
        alertJson = {"privilege_gain": 1, "ddos":2, "information_leak":3, \
                     "web_attack":4, "application_attack":5, "candc":6, \
                     "malware":7, "misc_attack":8, "other":9}

        time = timeStampToymd(time)
        nameList[0] = time
        alertType = alertType.lower()
        i = alertJson.get(alertType, '9')
        nameList[i] = num
        nameTuple = tuple(nameList)

        sqli = "call checkidscount%s"
        result = connMysql.insertOne(sqli, str(nameTuple))
        return result

def insertIdsMysql(connMysql, keystuple, valuestuple):
        newkeys = changeMysqlKeyTupleToStr(keystuple)
        newvalues = changeMysqlValueTupleToStr(valuestuple)

        sqli = "insert into alert_ids%s values%s"
        result = connMysql.insertOne(sqli , (str(newkeys), str(newvalues)))
        return result

def doIdsMysql(connMysql, keystuple, valuestuple, time, alertType):
        #pointname = 'idsPoint'
        s = string.ascii_lowercase
        pointname = string.join(random.sample(s,5)).replace(" ","")
        start_info = "start" + " " + pointname
        #doSetDebug(debugSign,"info","doVdsMysql",start_info)
        try:
                connMysql.begin(pointName=pointname)
                insertIdsMysql(connMysql, keystuple, valuestuple)
                changeIdsCountMysql(connMysql, time, alertType,1)
        except BaseException,err:
                doSetDebug(debugSign,"error","doIdsMysql",str(err))
                ids_info = str(keystuple) + "---" + str(valuestuple)
                doSetDebug(debugSign,"error","doIdsMysql",str(ids_info))
                connMysql.end(pointName=pointname,option="nocommit")
        finally:
                end_info = "end" + " " + pointname
                #doSetDebug(debugSign,"info","doVdsMysql",end_info)
#
# handle ids xdr, return (keys) and (values)
#
def handleIdsInfo(obj):
        global idsOKKeyList
        keyList = []
        valueList= []
        sipDict = obj.get("Src_ip_info",{})
        dipDict = obj.get("Dest_ip_info",{})
        
        skvDict = {"Country":"src_country","Province":"src_province","Organization":"src_org",\
                   "Network":"src_network","Lng":"src_longitude","Lat":"src_latitude",\
                   "TimeZone":"src_timezone","UTC":"src_utc","RegionalismCode":"src_regioncode",\
                   "PhoneCode":"src_phonecode","CountryCode":"src_countrycode","ContinentCode":"src_continentcode"}
        dkvDict = {"Country":"dest_country","Province":"dest_province","Organization":"dest_org",\
                   "Network":"dest_network","Lng":"dest_longitude","Lat":"dest_latitude",\
                   "TimeZone":"dest_timezone","UTC":"dest_utc","RegionalismCode":"dest_regioncode",\
                   "PhoneCode":"dest_phonecode","CountryCode":"dest_countrycode","ContinentCode":"dest_continentcode"}
        for k,v in sipDict.items():
                if k in idsOKKeyList:
                        keyList.append(skvDict.get(k))
                        valueList.append(v)
                        
        for k,v in dipDict.items():
                if k in idsOKKeyList:
                        keyList.append(dkvDict.get(k))
                        valueList.append(v)
        try:
                del obj["Src_ip_info"]
                del obj["Dest_ip_info"]
        except BaseException:
                doSetDebug(debugSign,"error","handleIdsInfo","delete ip info error")
        
        for k,v in obj.items():
                if k in idsOKKeyList:
                        keyList.append(k)
                        valueList.append(v)

        return tuple(keyList),tuple(valueList),obj["Time"],obj['Byzoro_type']

def doIdsPartition(iterList):
        s = string.ascii_lowercase
        sss = string.join(random.sample(s,5)).replace(" ","")
        sinfo = "start doIdsPartition " + sss
        einfo = "finally " + sss
        doSetDebug(debugSign,"info","doIdsPartition",sinfo)
        connMysql = Mysql()
        try:
                for iters in iterList:
                        keysTuple, valuesTuple, time, alertType = handleIdsInfo(iters)
                        #doSetDebug(debugSign,"info","doIdsPartition",str(valuesTuple))
                        if len(keysTuple) == len(valuesTuple) and len(keysTuple) > 0:
                                doIdsMysql(connMysql,keysTuple,valuesTuple,time,alertType)
                        else:
                                doSetDebug(debugSign,"warning","doIdsPartition","keys and values is not equit.")
                                doSetDebug(debugSign,"warning","doIdsPartition",str(iters))
        except BaseException,err:
                doSetDebug(debugSign,"error","doIdsPartition",str(err))
        finally:
                doSetDebug(debugSign,"info","doIdsPartition",einfo)
                connMysql.dispose()
#
# print xdr, for test
#
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
                                names[key] = initOffset(connMysql,"ids",top,partition)

                        topicPartion = TopicAndPartition(top, partition)
                        fromOffset[topicPartion] = long(names["offset%s%d" % (top,partition)])

        connMysql.dispose()
        return fromOffset
#
# start accept the values according to the streaming 
#
def startStreaming(obj, sign):
        #处理时间间隔为5s
        ssc=StreamingContext(sc, 5)

        if IDSCHECKPOINT:
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
        idsDstream = words.transform(changeIdsDict)
        #idsDstream.foreachRDD(rddToJson)
        idsDstream.foreachRDD(lambda rdd:rdd.foreachPartition(doIdsPartition))

        def getKafkaOffset(rdd):
                try:
                        offsetRange = rdd.offsetRanges()
                        for o in offsetRange:
                                key = "offset" + o.topic + str(o.partition)
                                obj[key] = o.untilOffset
                except BaseException,err:
                        doSetDebug(debugSign,"error","startStreaming",str(err))
        lines.foreachRDD(getKafkaOffset)

        if IDSCHECKPOINT:
                lines.checkpoint(checkpointInterval)
        return ssc

def checkInput(argv):
        global IDSCHECKPOINT
        try:
                opts, args = getopt.getopt(argv[1:], 'hc:', ['checkpoint='])
        except getopt.GetoptError, err:
                Usage()
                doSetDebug(debugSign,"error","checkInput","get input params failed !")
                sys.exit(2)
                
        for o, a in opts:
                if o in ('-h', '--help'):
                        Usage()
                        sys.exit(0)
                elif o in ('-c', '--checkpoint'):
                        if a == "yes":
                                IDSCHECKPOINT = 1
                        elif a == "no":
                                IDSCHECKPOINT = 0
                        else:
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
                                mysqlName = "ids" + top + str(partition)
                                key = "offset" + top + str(partition)
                                offset = newOffset.get(key,"")
                                info = mysqlName + "---" + str(offset)
                                if offset:
                                        updateOffsetMysql(connMysql, mysqlName, offset)

                connMysql.dispose()


