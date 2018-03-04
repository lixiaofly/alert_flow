# -*- coding: UTF-8 -*-
###spark streaming&&kafka
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
from pyspark import SparkContext,SparkConf
conf = SparkConf().setAppName("Kafka-Streaming FLOW")
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

FLOWCHECKPOINT = 1

if FLOWCHECKPOINT:
        checkpointDirectory = "/home/flow"
        checkpointInterval = 120
else:
        checkpointDirectory = ""
        checkpointInterval = 0
        
partitionNum = int(flowPartition)
topicList = [xdrTopic,httpTopic,fileTopic]

debugSign = "FLOW"
#
# xdr -> {"proto":proto,"sip":sip,"dip":dip,\
# "flowup":flowup,"flowdown":flowdown,"end":end}
#
def cutXdrDict(obj):
        #obj = json.loads(obj)
        newDict = dict()
        
        connDict = getConnDict(obj)
        connTimeDict = getConnTimeDict(obj)
        connStDict = getConnStDict(obj)
        
        newDict["proto"] = connDict.get("conn_proto")
        newDict["sip"] = connDict.get("conn_sip")
        newDict["dip"] = connDict.get("conn_dip")
        newDict["flowup"] = connStDict.get("cst_flup")
        newDict["flowdown"] = connStDict.get("cst_fld")
        newDict["end"] = connTimeDict.get("ctime_end")
        
        return newDict
#
# json.loads()
# xdr -> {"proto":proto,"sip":sip,"dip":dip,\
# "flowup":flowup,"flowdown":flowdown,"end":end}
#
def checkFlowJson(obj):
        try:
                err = json.loads(obj)
        except:
                err = ''
        finally:
                return err
        
def checkDict(obj):
        obj = checkFlowJson(obj)
        #print "************type:",type(obj)
        if isinstance(obj,dict):
                newDict = cutXdrDict(obj)
        else:
                newDict = ''
        return newDict

def checkTime(obj):
        if isinstance(obj,dict):
                endTime = obj.get('end', 0)
                if endTime == 0:
                        return False
                else:
                        return True
        else:
                return bool(0)
#
# json.loads
# xdr->{"proto":proto,"sip":sip,"dip":dip,"flowup":flowup,"flowdown":flowdown,"end":end}
#
def getNewDict(rdd):
        #print "getNewDict:",getNowTime()
        newRDD = rdd.map(lambda x:checkDict(x)).filter(lambda x:checkTime(x)).filter(lambda x:delNullRDD(x))
        #print "getNewDict end:",getNowTime()
        return newRDD
#
# get proto['tcp','udp']
#
def getProto(obj):
        return obj.get('proto','udp')
#
# get proto['tcp','udp']
#
def getSip(obj):
        sip = obj.get('sip','')
        return str(sip)

def getMTime(obj):
        atime = obj.get('end',0)
        return timeStampToymdM(atime)
#
# get flow number
#
def getFlow(obj):
        flowup = obj.get("flowup", 0)
        flowdown = obj.get("flowdown", 0)
        allflow = flowup + flowdown
        return (flowup, flowdown, allflow)
#
# get flow number
#
def getTimeFlow(obj):
        flowup = obj.get("flowup", 0)
        flowdown = obj.get("flowdown", 0)
        allflow = flowup + flowdown
        return (flowup, flowdown, allflow,1)
#
# [{obj},{obj},{obj}]->[((time,proto),(flow)),((time,proto),(flow)),((time,proto),(flow))]
#
def getProtoTuple(obj):
        if isinstance(obj, dict) and len(obj) > 0:
                return ((getMTime(obj), getProto(obj)),getFlow(obj))
        else:
                return ()

def getProtoRDD(rdd):
        tupleProtoRDD = rdd.map(lambda x:getProtoTuple(x))
        return tupleProtoRDD
#
# [{obj},{obj},{obj}]->[(time,(flow)),(time,(flow)),(time,(flow))]
#
def getTimeTuple(obj):
        if isinstance(obj, dict) and len(obj) > 0:
                return (getMTime(obj),getFlow(obj))
        else:
                return ()
def getTimeRDD(rdd):
        tupleProtoRDD = rdd.map(lambda x:getTimeTuple(x))
        return tupleProtoRDD
#
# [{obj},{obj},{obj}]->[((time,sip),(flow)),((time,sip),(flow)),((time,sip),(flow))]
#
def getSipTuple(obj):
        if isinstance(obj, dict) and len(obj) > 0:
                return ((getMTime(obj),getSip(obj)),getFlow(obj))
        else:
                return ()
def getSipRDD(rdd):
        tupleProtoRDD = rdd.map(lambda x:getSipTuple(x))
        return tupleProtoRDD
#
# [{obj},{obj},{obj}]->[((time,sip,proto),(flow)),((time,sip,proto),(flow)),((time,sip,proto),(flow))]
#
def getSipProtoTuple(obj):
        if isinstance(obj, dict) and len(obj) > 0:
                return((getMTime(obj),getSip(obj),getProto(obj)),getTimeFlow(obj))
        else:
                return ()

def getSipProtoRDD(rdd):
        tupleProtoRDD = rdd.map(lambda x:getSipProtoTuple(x))
        return tupleProtoRDD	
#
# (a,b,c)+(x,y,x) -> (a+x,b+y,c+z)
#
def countflow(a,b):
        if not a:
                a = (0,0,0)
        if not b:
                b = (0,0,0)
        a,b = tuple(a),tuple(b)
        if len(a) == 1:
                a = a[0]
        if len(b) == 1:
                b = b[0]
        return tuple([x+y for x,y in zip(a,b)])
#
# (a,b,c)+(x,y,x) -> (a+x,b+y,c+z)
#
def countTimeFlow(a,b):
        if not a:
                a = (0,0,0,0)
        if not b:
                b = (0,0,0.0)
        a,b = tuple(a),tuple(b)
        if len(a) == 1:
                a = a[0]
        if len(b) == 1:
                b = b[0]
        return tuple([x+y for x,y in zip(a,b)])
#
# print rdd.collect
#
def rddToJson(rdd):
        print rdd.collect()
#
# tableName,[(values),(values)]->[(tableName,values),(tableName,values)]
#
def joinTblName(name,obj):
        nameTuple = (name,)
        newObj = []
        for iters in obj:
                iters = nameTuple + iters
                newObj.append(iters)
        return newObj
#
# insert direct mysql
#
def getDirectDict(obj):
        time = obj[0]
        upflow, downflow, allflow = obj[1]
        return [(time,upflow,'up'),(time,downflow,'down')]

def insertDirectMysql(connMysql, obj):
        for iters in obj:
                sqli = "call checknetflowd%s"
                connMysql.insertOne(sqli, str(iters))

#
# insert protocol mysql
#
def getProtoDict(obj):
        time, proto = obj[0]
        upflow, downflow, allflow = obj[1]
        return [(time,allflow,proto)]

def insertProtoMysql(connMysql, obj):
        for iters in obj:
                sqli = "call checknetflowp%s"
                connMysql.insertOne(sqli, str(iters))
#
# insert direction and protocol mysql
#
def getDirectProtoDict(obj):
        time, proto = obj[0]
        upflow, downflow, allflow = obj[1]
        return [(time,upflow,'up',proto),(time,downflow,'down',proto)]

def insertDirectProtoMysql(connMysql, obj):
        for iters in obj:
                sqli = "call checknetflowdp%s"
                connMysql.insertOne(sqli, str(iters))
#
# insert sip mysql
#
def getSipDict(obj):
        time,sip = obj[0]
        upflow, downflow, allflow = obj[1]
        return [(time, allflow, sip)]

def insertSipMysql(connMysql, obj):
        for iters in obj:
                sqli = "call checknetflowip%s"
                connMysql.insertOne(sqli, str(iters))
#
# insert sip and direction mysql
#
def getSipDirectDict(obj):
        time,sip = obj[0]
        upflow, downflow, allflow = obj[1]
        # change by liu
        return [(time, upflow, 'up', sip), (time, downflow, 'down', sip)]
        #	return [(time, upflow, sip, 'up'), (time, downflow, sip, 'down')]

def insertSipDirectMysql(connMysql, obj):
        for iters in obj:
                sqli = "call checknetflowipd%s"
                connMysql.insertOne(sqli, str(iters))
#
# insert sip and protocol mysql
#
def getSipProtoDict(obj):
        time,sip,proto = obj[0]
        _, _, allflow, _ = obj[1]
        return [(time, allflow, sip, proto)]

def insertSipProtoMysql(connMysql, obj):
        for iters in obj:
                sqli = "call checknetflowipp%s"
                connMysql.insertOne(sqli, str(iters))

#
# insert sip direction and protocol mysql
#
def getSipDirectProtoDict(obj):
        time,sip,proto = obj[0]
        upflow, downflow, allflow, num = obj[1]
        return [(time,upflow,'up',sip,proto,num),(time,downflow,'down',sip,proto,num)]

def insertSipDirectProtoMysql(connMysql, obj):
        #doSetDebug(debugSign,"info","insertSipDirectProtoMysql",str(obj))
        for iters in obj:
                sqli = "call checknetflow%s"
                connMysql.insertOne(sqli,str(iters))
#
# foreachPartition proto (insertProtoMysql, insertDirectProtoDict)
#
def doProtoPartition(iterList):
        connMysql = Mysql()

        try:
                for iters in iterList:
                        protoList = getProtoDict(iters)
                        insertProtoMysql(connMysql, tuple(protoList))
                        directProtoList = getDirectProtoDict(iters)
                        insertDirectProtoMysql(connMysql, tuple(directProtoList))
        except BaseException,err:
                doSetDebug(debugSign,"error","doProtoPartition",str(err))
        finally:
                connMysql.dispose()

#
# foreachPartition time (insertDirectMysql)
#
def doTimePartition(iterList):
        connMysql = Mysql()

        try:
                for iters in iterList:
                        directList = getDirectDict(iters)
                        insertDirectMysql(connMysql, tuple(directList))
                        #allDirectList.extend(directList)
        except BaseException,err:
                doSetDebug(debugSign,"error","doTimePartition",str(err))
        finally:
                connMysql.dispose()

#
# foreachPartition sip (insertDirectMysql)
#
def doSipPartition(iterList):
        connMysql = Mysql()

        try:
                for iters in iterList:
                        sipList = getSipDict(iters)
                        insertSipMysql(connMysql, tuple(sipList))
                        sipDirectList = getSipDirectDict(iters)
                        insertSipDirectMysql(connMysql, tuple(sipDirectList))
        except BaseException,err:
                doSetDebug(debugSign,"error","doSipPartition",str(err))
        finally:
                connMysql.dispose()
#
# foreachPartition sip and protocol (insertSipProtoMysql)
#
def doSipProtoPartition(iterList):
        connMysql = Mysql()

        try:
                for iters in iterList:
                        sipProtoList = getSipProtoDict(iters)
                        insertSipProtoMysql(connMysql, tuple(sipProtoList))
                        sipDirectProtoList = getSipDirectProtoDict(iters)
                        insertSipDirectProtoMysql(connMysql, tuple(sipDirectProtoList))
        except BaseException,err:
                doSetDebug(debugSign,"error","doSipProtoPartition",str(err))
        finally:
                connMysql.dispose()
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
                                names[key] = initOffset(connMysql,"netflow",top,partition)

                        topicPartion = TopicAndPartition(top, partition)
                        fromOffset[topicPartion] = long(names["offset%s%d" % (top,partition)])

        connMysql.dispose()
        return fromOffset
#
# start accept the values according to the streaming 
#
def startStreaming(obj, sign):
        #处理时间间隔为5s
        ssc = StreamingContext(sc, 30)
        
        if FLOWCHECKPOINT:
                ssc.checkpoint(checkpointDirectory)
        '''
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
                                                      kafkaParams={"metadata.broker.list": brokers},\
                                                      fromOffsets=fromOffset)
        '''
        lines = KafkaUtils.createStream(ssc,zook,"netflow",{xdrTopic:flowPartition, httpTopic:flowPartition, fileTopic:flowPartition})

        lines1=lines.map(lambda x:x[1])
        words=lines1.flatMap(lambda line:line.split("\n"))
        #words.pprint()
        #streamRDD->RDD->list[json]
        # xdr->{"proto":proto,"sip":sip,"dip":dip,"flowup":flowup,"flowdown":flowdown,"end":end}
        #	
        ds = words.transform(getNewDict)
        #ds.pprint()
        #
        # reduce by proto and time
        #
        dsProto1 = ds.transform(getProtoRDD)
        dsProto = dsProto1.reduceByKeyAndWindow(countflow,None,60,slideDuration=60,numPartitions=3)
        #dsProto.foreachRDD(rddToJson)
        #
        # reduce by time
        #
        dsTime1 = ds.transform(getTimeRDD)
        dsTime = dsTime1.reduceByKeyAndWindow(countflow,None,60,slideDuration=60,numPartitions=3)
        #dsTime1.foreachRDD(rddToJson)
        #
        # reduce by sip
        #
        dsSip1 = ds.transform(getSipRDD)
        dsSip = dsSip1.reduceByKeyAndWindow(countflow,None,60,slideDuration=60,numPartitions=3)
        #
        # reduce by sip and proto
        #
        dsSipProto1 = ds.transform(getSipProtoRDD)
        dsSipProto = dsSipProto1.reduceByKeyAndWindow(countTimeFlow,None,60,slideDuration=60,numPartitions=3)	
        #
        # action
        #
        dsProto.foreachRDD(lambda rdd: rdd.foreachPartition(doProtoPartition))
        dsTime.foreachRDD(lambda rdd: rdd.foreachPartition(doTimePartition))
        dsSip.foreachRDD(lambda rdd: rdd.foreachPartition(doSipPartition))
        dsSipProto.foreachRDD(lambda rdd: rdd.foreachPartition(doSipProtoPartition))
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
        if FLOWCHECKPOINT:
                lines.checkpoint(checkpointInterval)
        return ssc

def checkInput(argv):
        global FLOWCHECKPOINT
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
                                FLOWCHECKPOINT = 1
                        elif a == "no":
                                FLOWCHECKPOINT = 0
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
	'''
        finally:
                connMysql = Mysql()
                for top in topicList:
                        for partition in range(partitionNum):
                                mysqlName = "netflow" + top + str(partition)
                                key = "offset" + top + str(partition)
                                offset = newOffset.get(key,"")
                                info = mysqlName + "---" + str(offset)
                                if offset:
                                        updateOffsetMysql(connMysql, mysqlName, offset)

                connMysql.dispose()
	'''

