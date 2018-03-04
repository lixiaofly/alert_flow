# -*- coding: UTF-8 -*-
###spark streaming&&kafka
import string,random
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
conf = SparkConf().setAppName("Kafka-Streaming VDS")
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
from MySqlConn import Mysql
from docommand import *
from doDebug import doSetDebug

VDSCHECKPOINT = 1

if VDSCHECKPOINT:
        checkpointDirectory = "/home/vds"
        checkpointInterval = 120
else:
        checkpointDirectory = ""
        checkpointInterval = 0
        
partitionNum = int(vdsPartition)
topicList = [vdsTopic]
debugSign = "VDS"

def getNewVdsXdrDict(obj):
        if isinstance(obj, dict) and len(obj) > 0:
                #newXdr = getOneRowXdr(obj)
                newXdr = getNewXdr(obj)
                newXdr['Alert'] = obj['Alert']
                return newXdr
        else:
                return ''
	
def changeVdsXdrDict(obj):
        doSetDebug(debugSign,"info","changeVdsXdrDict","start changeVdsXdrDict")
        rdd = checkXdrDict(obj)
        newRDD = rdd.map(lambda x:getNewXdrDict(x)).filter(lambda x:delNullRDD(x))
        return newRDD

def changeVdsCountMysql(connMysql, time, alerttype, num):
        nameList = [0,0,0,0,0,0,0,0,0,0,0,0,0]
        alertJson = {"backdoor": 1, "trojan":2, "risktool":3, "spyware":4, "malware":5,\
                     "virus":6, "worm":7, "joke":8, "adware":9, "hacktool":10,\
                     "exploit":11, "other":12}
        #
        # (time,backdoor,trojan,risktool,spyware,malware,virus,worm,joke,adware,hacktool,exploit,other)
        #
        sqli = "call checkvdscount%s"

        time = timeStampToymd(time)
        nameList[0] = time
        alerttype = alerttype.lower()
        i = alertJson.get(alerttype, '12')
        nameList[i] = num
        nameTuple = tuple(nameList)
        result  = connMysql.insertOne(sqli, str(nameTuple))
        return result
#
# insert into alert table
#
def insertVdsMysql(connMysql, keystuple, valuestuple, tag):
        newkeys = changeMysqlKeyTupleToStr(keystuple)
        newvalues = changeMysqlValueTupleToStr(valuestuple)

        if tag == "offline":
                sqli = "insert into alert_vds_offline%s values%s"
        else:
                sqli = "insert into alert_vds%s values%s"
        result = connMysql.insertOne(sqli,(newkeys, newvalues))
        return result

def doVdsMysql(connMysql, vdsKeys, vdsValues, time, alerttype, xdrKeys, xdrValues, tag):
        #pointname = 'vdsPoint'
        s = string.ascii_lowercase
        pointname = string.join(random.sample(s,5)).replace(" ","")
        start_info = "start" + " " + pointname
        #doSetDebug(debugSign,"info","doVdsMysql",start_info)
        try:
                connMysql.begin(pointName=pointname)
                alert_id = insertVdsMysql(connMysql, vdsKeys, vdsValues, tag)
                insertXdrMysql(connMysql, xdrKeys, xdrValues, alert_id, tag, "VDS")
                if tag != "offline":
                        changeVdsCountMysql(connMysql, time, alerttype, 1)
        except BaseException,err:
                doSetDebug(debugSign,"error","doVdsMysql",str(err))
                vds_info=str(vdsKeys) + "---" + str(vdsValues)
                xdr_info=str(xdrKeys) + "---" + str(xdrValues)
                doSetDebug(debugSign,"error","doVdsMysql",str(vds_info))
                doSetDebug(debugSign,"error","doVdsMysql",str(xdr_info))
                connMysql.end(pointName=pointname,option="nocommit")
        finally:
                end_info = "end" + " " + pointname
                #doSetDebug(debugSign,"info","doVdsMysql",end_info)
#
# handle vds xdr, return (keys) and (values)
#
def handleVdsInfo(obj):
        global connList,vdsOKKeyList
        alertInfo = obj['Alert']
        keyList = []
        valueList = []
        for k,v in alertInfo.items():
                if k in vdsOKKeyList:
                        keyList.append(k)
                        valueList.append(v)
        
        addKeyList = list()
        addValueList = list()
        
        vdsAddInfoDict = {"conn_scountry":"src_country","conn_sprovince":"src_province","conn_dregioncode":"",\
                          "conn_slat":"src_latitude","conn_slng":"src_longitude","conn_dcountry":"dest_country",\
                          "conn_dprovince":"dest_province","conn_dlat":"dest_latitude","conn_dlng":"dest_longitude",\
                          "conn_sip":"src_ip","conn_dip":"dest_ip","conn_sport":"src_port",\
                          "conn_dport":"dest_port","conn_proto":"proto","app_file":"app_file",\
                          "http_url":"http_url"}
        vdsAddInfoList = ["conn_scountry","conn_sprovince","conn_dregioncode","conn_slat","conn_slng",\
                          "conn_dcountry","conn_dprovince","conn_dregioncode","conn_dlat","conn_dlng",\
                          "conn_sip","conn_dip","conn_sport","conn_dport","conn_proto",\
                          "app_file","http_url"]

        for name in vdsAddInfoList:
                v = obj.get(name,"")
                #info = str(name) + "=" + str(v)
                #doSetDebug(debugSign,"info","handleWafInfo",str(info))
                k = vdsAddInfoDict.get(name)
                if v != "" and k != "":
                        addKeyList.append(k)
                        addValueList.append(str(v))

        keyList.extend(addKeyList)
        valueList.extend(addValueList)
        
        time = obj.get("time")
        keyList.append("time")
        valueList.append(time)
        
        tag = obj.get('offline_tag', 'online')
        if tag == "offline":
                task_id = obj.get("task_id",0)
                keyList.append("task_id")
                valueList.append(task_id)

        return tuple(keyList),tuple(valueList),time,alertInfo.get('Local_vtype')
#
# handle xdr info return (keys) and (values)
#
def handleXdrInfo(obj):
        #global xdrOKKeyList
        keyList = []
        valueList =[]
        tag = obj.get('offline_tag', 'online')
        #if tag != "offline":
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

def doVdsPartition(iterList):
        s = string.ascii_lowercase
        sss = string.join(random.sample(s,5)).replace(" ","")
        sinfo = "start doVdsPartition " + sss
        einfo = "finally " + sss
        doSetDebug(debugSign,"info","doVdsPartition",sinfo)
        connMysql = Mysql()
        try:
                for iters in iterList:
                        vdsKeys, vdsValues, time, alerttype = handleVdsInfo(iters)
                        #doSetDebug(debugSign,"info","doVdsPartition",str(vdsValues))
                        del iters['Alert']
                        xdrKeys, xdrValues, tag = handleXdrInfo(iters)
                        if len(vdsKeys) == len(vdsValues) and len(vdsKeys) > 0:
                                doVdsMysql(connMysql, vdsKeys, vdsValues, time, \
                                           alerttype, xdrKeys, xdrValues, tag)
                        else:
                                doSetDebug(debugSign,"warning","doVdsPartition","keys and values is not equit.")
                                doSetDebug(debugSign,"warning","doVdsPartition",str(iters))
        except BaseException,err:
                doSetDebug(debugSign,"error","doVdsPartition",str(err))
        finally:
                doSetDebug(debugSign,"info","doVdsPartition",einfo)
                connMysql.dispose()
                
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
                                names[key] = initOffset(connMysql,"vds",top,partition)

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

        if VDSCHECKPOINT:
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
	
        vdsDstream = words.transform(changeVdsXdrDict)
        #vdsDstream.foreachRDD(rddToJson)
        vdsDstream.foreachRDD(lambda rdd:rdd.foreachPartition(doVdsPartition))

        def getKafkaOffset(rdd):
                #if not rdd.isEmpty():
                try:
                        offsetRange = rdd.offsetRanges()
                        for o in offsetRange:
                                key = "offset" + o.topic + str(o.partition)
                                obj[key] = o.untilOffset
                except BaseException,err:
                        doSetDebug(debugSign,"error","startStreaming",str(err))
        lines.foreachRDD(getKafkaOffset)
        
        if VDSCHECKPOINT:
                lines.checkpoint(checkpointInterval)
        return ssc

def checkInput(argv):
        global VDSCHECKPOINT
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
                                VDSCHECKPOINT = 1
                        elif a == "no":
                                VDSCHECKPOINT = 0
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
        except BaseException,err:
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
                                mysqlName = "vds" + top + str(partition)
                                key = "offset" + top + str(partition)
                                offset = newOffset.get(key,"")
                                info = mysqlName + "---" + str(offset)
                                if offset:
                                        updateOffsetMysql(connMysql, mysqlName, offset)

                connMysql.dispose()


