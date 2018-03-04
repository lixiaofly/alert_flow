# -*- coding: UTF-8 -*-
###spark streaming&&kafka
import string,random
import json
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
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils,TopicAndPartition

from pyspark import SparkContext,SparkConf
conf = SparkConf().setAppName("PORT SCAN")
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

from threatCommand import *
#from threatMysql import *
from MySqlConn import Mysql
from docommand import *
from domysql import *
from doDebug import doSetDebug,doGetDebug

PSCANCHECKPOINT = 1

if PSCANCHECKPOINT:
        checkpointDirectory = "/home/portscan"
        checkpointInterval = 120
else:
        checkpointDirectory = ""
        checkpointInterval = 0

topicList = [portScanTopic]
partitionNum = int(portScanPartition)

debugSign = "SCAN"

allConnList = list()
semiConnList = list()
semiCount = 0
allCount = 0
dportList = list(portlist)
#
# get conn time
#
def getConnTime(obj):
        start, end = obj.get('ctime_start',0), \
                        obj.get('ctime_end',0)
        return int(end) - int(start)
#
# check conn time 
#
def checkConnTime(obj):
        conntime = getConnTime(obj)
        if conntime < 1:
                return 1
        else:
                return 0
#
# check conn flow
#
def checkConnFlow(obj):
        flowUp, flowDown = obj.get("sst_flup", 0), \
                        obj.get("sst_fld",0) 
        if flowUp < 200 and flowDown < 200:
                return 1
        else:
                return 0
#
# check up package count
#
def checkUpPkt(obj):
        pktUp = obj.get("sst_pktup",0)
        if pktUp < 5:
                return 1
        else:
                return 0
#
# get time,sip,dport
#
def gethost(obj,scantype):
        if scantype != "source":
                host = obj.get("conn_dip", "")
        else:
                host = obj.get("conn_sip", "")
        return host

def getport(obj):
        global dportList
        dport = obj.get("conn_dport", "")
        if dport in dportList:
                port = "internal"
        else:
                port = "external"
        return port
#
# get insert time (30s)
#
def getinsertTime(obj):
        global scanInterval
        xdrtime = obj.get("time", 0)
        mintime = timeStampToymdM(xdrtime)
        atime = xdrtime - mintime
        if atime > scanInterval:
                time = mintime + scanInterval
        else:
                time = mintime
        return time
#
# (time[30s], sip, dip, port["internal"/"external"])
#
def getKeys(obj, scantype):
        time = getinsertTime(obj)
        host = gethost(obj, scantype)
        port = getport(obj)

        return time, host, port
#
# check all connect
#
def checkallConn(obj, scantype):
        connType = "allconn"
        err = checkConnTime(obj) + checkConnFlow(obj) + checkUpPkt(obj)
        if err == 0 :
                return ''
        else:
                time,host,port = getKeys(obj, scantype)
                keys = (time,connType,host,scantype,port)
                return (keys, obj)
        
#
# do check All-connected
#
def checkSemiConn(obj):
        tcp_Handshake12, tcp_Handshake23 = obj.get("tcp_hndshk12",0),\
                obj.get("tcp_hndshk23",0)
                #doSetDebug(debugSign,"info","checkSemiConn",str(tcp_Handshake12))
                #doSetDebug(debugSign,"info","checkSemiConn",str(tcp_Handshake23)) 
        if tcp_Handshake12 == 1 and tcp_Handshake23 == 0:
                connType, scantype = "semiconn","source"
                time,host,port = getKeys(obj,scantype)
                newkeys = (time,connType,host,scantype,port)
                doSetDebug(debugSign,"info","checkSemiConn",str(newkeys))
                return (newkeys, obj)
        else:
                return ''
#
# check all connection of source
#
def doCheckDallConn(rdd):
        newRDD = rdd.map(lambda x:checkallConn(x, "direct")).filter(lambda x:delNullRDD(x))
        return newRDD
#
# check all connection of source
#
def doCheckSallConn(rdd):
        newRDD = rdd.map(lambda x:checkallConn(x, "source")).filter(lambda x:delNullRDD(x))
        return newRDD
#
# check semi connection
#
def doCheckSemiConn(rdd):
        newRDD = rdd.map(lambda x:checkSemiConn(x)).filter(lambda x:delNullRDD(x))
        return newRDD

#
# check dip: instrance->True, extrance->False
#
def checkConnDip(connDip):
        if connDip != '':
                return is_internal_ip(connDip)
        else:
                return False
#
# check dip
#
def checkDip(obj):
        connDip = obj.get("conn_dip","")
        dip = checkConnDip(connDip)
        #doSetDebug(debugSign,"info","checkDip",str(dip))
        if dip == True:
                return obj
        else:
                return ''
                
def doCheckDip(rdd):
        newRDD = rdd.map(lambda x:checkDip(x)).filter(lambda x:delNullRDD(x))

        return newRDD
#
# json.loads
#
def checkConnJson(obj):
        try:
                err = json.loads(obj)
        except BaseException, e:
                e = "json loads failed. " + str(e)
                doSetDebug(debugSign,"error","checkConnJson",str(e))
                err = ''
        finally:
                return err
#
# check vds alert dict
#
def checkConnXdrDict(rdd):
        newRDD = rdd.map(lambda x:checkConnJson(x)).filter(lambda x:delNullRDD(x))
        return newRDD

def getNewConnXdrDict(obj):
        if isinstance(obj, dict) and len(obj) > 0:
                try:
                        newXdr = getNewXdr(obj)
                except BaseException,err:
                        err = "handleXdr error:" + str(err)
                        doSetDebug(debugSign,"error","getNewConnXdrDict",str(err))
                        newXdr = {}
                return newXdr
        else:
                return {}
	
def changeConnXdrDict(obj):
        doSetDebug(debugSign,"info","changeConnXdrDict","start changeConnXdrDict")
        rdd = checkConnXdrDict(obj)
        newRDD = rdd.map(lambda x:getNewConnXdrDict(x))
        return newRDD
#
# handle xdr info return (keys) and (values)
#
def handleXdrInfo(obj):
        #global xdrOKKeyList
        keyList = list()
        valueList = list()
        
        tag = obj.get("offline_tag","null")
        task_id = obj.get("task_id","null")
        
        if tag != "null":
                del obj['offline_tag']

        if task_id != "null":
                del obj['task_id']

        for k,v in obj.items():
                keyList.append(k)
                valueList.append(v)

        return tuple(keyList), tuple(valueList), "portscan"

def insertConnMysql(connMysql, keys, values, count):
        newvalues = list()
        for iters in values:
                newvalues.append(str(iters))
        newvalues.append(str(count))

        newvalues = tuple(newvalues)
        value = "\',\'".join(newvalues)
        newvalues = str('(\'') + str(value) + str('\', @paraid)')

        try:
                sqli = "call checkalertportscan%s"
                alert_id = connMysql.insertReturn(sqli,str(newvalues),"@paraid")
                #sqli = "insert into alert_portscan%s values%s"
                #alert_id = connMysql.insertOne(sqli,(str(keys),str(newvalues)))
        except BaseException,err:
                doSetDebug(debugSign,"error","insertConnMysql",str(err))
                alert_id = 0
                
        return alert_id

def doInsertXdrMysql(connMysql, value, alert_id, tag, alert_type):
        keyTuple, valueTuple, tag = handleXdrInfo(value)
        insertXdrMysql(connMysql, keyTuple, valueTuple, alert_id, tag, alert_type)

def doAllMysql(connMysql,alertkeys,alertvalues,xdrvalues,tag):
        global scanInterval
        (time,connType,host,scantype,port) = alertvalues
        newalertvalues = alertvalues[:4]
        if port == "internal":
                num = scanInterval * 20
        else:
                num = scanInterval * 5
        vlen= len(xdrvalues)
        if isinstance(xdrvalues,list) and vlen > num:
                Alert_Id = insertConnMysql(connMysql,alertkeys,newalertvalues,vlen)
                for value in xdrvalues:
                        doInsertXdrMysql(connMysql,value,Alert_Id,tag,"allconn")

def doSemiMysql(connMysql,alertkeys,alertvalues,xdrvalues,tag):
        global scanInterval

        s = string.ascii_lowercase
        pointname = string.join(random.sample(s,5)).replace(" ","")
        start_info = "start" + " " + pointname
        #doSetDebug(debugSign,"info","doSemiMysql",start_info)
        
        (time,connType,host,scantype,port) = alertvalues
        newalertvalues = alertvalues[:4]
        if isinstance(xdrvalues,dict):
                Alert_Id = insertConnMysql(connMysql,alertkeys,newalertvalues,1)
                doInsertXdrMysql(connMysql,xdrvalues,Alert_Id,tag,connType)
        else:
                vlen = len(xdrvalues)
                Alert_Id = insertConnMysql(connMysql,alertkeys,newalertvalues,vlen)
                for value in xdrvalues:
                        doInsertXdrMysql(connMysql,value,Alert_Id,tag,connType)

        end_info = "end" + " " + pointname
        #doSetDebug(debugSign,"info","doSemiMysql",end_info)
#
# do foreachPartition
#
def doConnPartition(iterList,scantype):
        #s = string.ascii_lowercase
        #sss = string.join(random.sample(s,5)).replace(" ","")
        #sinfo = "start doConnPartition " + scantype + " " + sss
        #einfo = "finally " + sss
        #doSetDebug(debugSign,"info","doConnPartition",sinfo)
        connMysql = Mysql()
        alertkeys = '(time,conntype,host,scantype,count)'
        try:
                if scantype == "source" or scantype == "direct":
                        for iters in iterList:
                                alertvalues,xdrvalues = iters[0],iters[1]
                                doAllMysql(connMysql, alertkeys, alertvalues, xdrvalues, "portscan")
                else:
                        for iters in iterList:
                                alertvalues,xdrvalues = iters[0],iters[1]
                                doSemiMysql(connMysql, alertkeys, alertvalues, xdrvalues, "portscan")
        except BaseException,err:
                doSetDebug(debugSign,"error","doConnPartition",str(err))
        finally:
                #doSetDebug(debugSign,"info","doConnPartition",einfo)
                connMysql.dispose()
#
# reduceByKeyAndWindow
#
def doreduce(a,b):
        if not isinstance(a,list):
                a = [a]
	
        if not b:
                b = {}
        if not isinstance(b,list):
                b = [b]
        a.extend(b)
        return a
#
# test print
#
def rddToJson(rdd):
        print rdd.collect()

#
# get offset according toopic and partition
#
def getFromOffset():
        names = locals()
        fromOffset = {}
        connMysql = Mysql()
        for top in topicList:
                for partition in range(partitionNum):
                        topicPartion = TopicAndPartition(top, partition)
                        try:
                                names["offset%s%d" % (top,partition)]
                        except:
                                names["offset%s%d" % (top,partition)] = \
                                        initOffset(connMysql, "portscan", top, partition)
                        fromOffset[topicPartion] = \
                                long(names["offset%s%d" % (top,partition)])
        connMysql.dispose()
        return fromOffset
#
# start accept the values according to the streaming 
#
def startStreaming(obj,sign):
        global scanInterval
        #checkfile = "/tmp/portscan/"
        #处理时间间隔为5s
        ssc=StreamingContext(sc, 10)

        if PSCANCHECKPOINT:
                ssc.checkpoint(checkpointDirectory)
        #ssc.checkpoint(checkfile)
        '''
        if sign == "new":
                doSetDebug(debugSign,"warning","createDirectStream","start from mysql offset failed.")
                doSetDebug(debugSign,"warning","createDirectStream","start from new kafka offset.")
                lines = KafkaUtils.createDirectStream(ssc, \
                                                      topicList, \
                                                      kafkaParams={"metadata.broker.list": brokers})
        else:
                fromOffset = getFromOffset()
                doSetDebug(debugSign,"info","startStreaming",str(fromOffset))
                doSetDebug(debugSign,"info","startStreaming","start from mysql offset.")
                lines = KafkaUtils.createDirectStream(ssc, \
                                                      topicList, \
                                                      kafkaParams={"metadata.broker.list": brokers},\
                                                      fromOffsets=fromOffset)

        '''
        lines = KafkaUtils.createStream(ssc,zook,"portscan",{portScanTopic:partitionNum})
        lines1=lines.map(lambda x:x[1]) #注意 取tuple下的第二个即为接收到的kafka流
        #对5s内收到的字符串进行分割
        words=lines1.flatMap(lambda line:line.split("\n"))
        #words.pprint()
        #streamRDD->RDD->list[json]
        connDstream1 = words.transform(changeConnXdrDict)
        connDstream2 = connDstream1.transform(doCheckDip)
        semiDstream1 = connDstream2.transform(doCheckSemiConn)
        sallDstream1 = connDstream2.transform(doCheckSallConn)
        dallDstream1 = connDstream2.transform(doCheckDallConn)
        
        semiDstream = semiDstream1.reduceByKeyAndWindow(doreduce,None,scanInterval,slideDuration=scanInterval,numPartitions=3)
        sallDstream = sallDstream1.reduceByKeyAndWindow(doreduce,None,scanInterval,slideDuration=scanInterval,numPartitions=3)
        dallDstream = dallDstream1.reduceByKeyAndWindow(doreduce,None,scanInterval,slideDuration=scanInterval,numPartitions=3)
        #semiDstream.foreachRDD(rddToJson)
        #dallDstream.foreachRDD(rddToJson)
        #sallDstream.foreachRDD(rddToJson)
        semiDstream.foreachRDD(lambda rdd:rdd.foreachPartition(lambda obj:doConnPartition(obj,"")))
        sallDstream.foreachRDD(lambda rdd:rdd.foreachPartition(lambda obj:doConnPartition(obj,"source")))
        dallDstream.foreachRDD(lambda rdd:rdd.foreachPartition(lambda obj:doConnPartition(obj,"direct")))
        '''
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
        '''
        if PSCANCHECKPOINT:
                lines.checkpoint(checkpointInterval)
        return ssc

def checkInput(argv):
        global PSCANCHECKPOINT
        try:
                opts, args = getopt.getopt(argv[1:], 'hc:', ['checkpoint='])
        except getopt.GetoptError, err:
                Usage()
                sys.exit(2)
                
        for o, a in opts:
                if o in ('-h', '--help'):
                        Usage()
                        sys.exit(0)
                elif o in ('-c', '--checkpoint'):
                        if a == "yes":
                                PSCANCHECKPOINT = 1
                        elif a == "no":
                                PSCANCHECKPOINT = 0
                        else:
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

if __name__ == "__main__":
        checkInput(sys.argv)
        
        newOffset = dict()
        err = delCheckPoint(checkpointDirectory)
        if err:
                sys.exit(2)
        try:
                startOffset(newOffset)
        except BaseException,e:
                doSetDebug(debugSign,"error","main",str(e))
        '''
        finally:
                connMysql = Mysql()
                for top in topicList:
                        for partition in range(partitionNum):
                                mysqlName = "portscan" + top + str(partition)
                                key = "offset" + top + str(partition)
                                offset = newOffset.get(key,"")
                                info = mysqlName + "---" + str(offset)
                                if offset:
                                        updateOffsetMysql(connMysql, mysqlName, offset)

                connMysql.dispose()
        '''

