#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pykafka.client import KafkaClient
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import datetime
import sys,os,stat
CUR_PATH = os.path.split(os.path.realpath(sys.argv[0]))[0]
sys.path.append("%s/../conf" % CUR_PATH)
sys.path.append("%s/../pkg" % CUR_PATH)
reload(sys)
sys.setdefaultencoding('utf-8')

from docommand import *
from threatCommand import *
#
# CRITICAL > ERROR > WARNING > INFO > DEBUG > NOTSET
#

debugDict = {"IDS":idsDebug,"VDS":vdsDebug,"WAF":wafDebug,"Flow":flowDebug,"SCAN":scanDebug}
debugTopicDict = {"IDS":idsDebugTopic,"VDS":vdsDebugTopic,"WAF":wafDebugTopic,"FLOW":flowDebugTopic,"SCAN":scanDebugTopic}
debugFileDict = {"IDS":"/tmp/ids.log","VDS":"/tmp/vds.log","WAF":"/tmp/waf.log","FLOW":"/tmp/flow.log","SCAN":"/tmp/scan.log"}
debugGroupIdDict = {"IDS":"ids-debug","VDS":"vds-debug","WAF":"waf-debug","FLOW":"flow-debug","SCAN":"scan-debug"}

def getFileName(sign):
        pathTime = datetime.datetime.now().strftime('%Y-%m-%d')
        fileTime = datetime.datetime.now().strftime('%H')
        path = "/tmp/" + sign.lower() + "-" + str(pathTime)
        fileName = str(fileTime)
        outfile = path + "/" + fileName
        return outfile

def getLogInfo(level,func,obj):
        nowTime = getNowTime()
        info = nowTime + " " + level + " " + func + " " + str(obj)
        return info

class Kafka_producer():
        '''
        使用kafka的生产模块
        '''
        def __init__(self, kafkahost,kafkaport, kafkatopic):
                self.kafkaHost = kafkahost
                self.kafkaPort = kafkaport
                self.kafkatopic = kafkatopic
                self.producer = KafkaProducer(bootstrap_servers = '{kafka_host}:{kafka_port}'.format(\
                                kafka_host=self.kafkaHost,\
                                kafka_port=self.kafkaPort\
                                ))

        def sendjsondata(self, params):
                try:
                        parmas_message = json.dumps(params)
                        producer = self.producer
                        producer.send(self.kafkatopic, parmas_message.encode('utf-8'))
                        producer.flush()
                except KafkaError as e:
                        print e

class Kafka_client():
        def __init__(self, brokers,kafkatopic):
                self.brokers = brokers
                self.kafkatopic = kafkatopic
                self.client = KafkaClient(brokers)
                self.topic = self.client.topics[kafkatopic]
                self.producer = self.topic.get_sync_producer()
                
        def sendmsg(self, params):
                producer = self.producer
                try:
                        parmas_message = json.dumps(params)
                        #for x in parmas_message:
                        producer.produce(parmas_message)
                except BaseException,err:
                        print str(err)
                finally:
                        producer.stop()

class Kafka_consumer():
        '''
        使用Kafka—python的消费模块
        '''
        def __init__(self, kafkahost, kafkaport, kafkatopic, groupid):
                self.kafkaHost = kafkahost
                self.kafkaPort = kafkaport
                self.kafkatopic = kafkatopic
                self.groupid = groupid
                self.consumer = KafkaConsumer(self.kafkatopic, group_id = self.groupid,\
                                      bootstrap_servers = '{kafka_host}:{kafka_port}'.format(\
                                                      kafka_host=self.kafkaHost,\
                                                      kafka_port=self.kafkaPort ))

        def consume_data(self):
                try:
                        for message in self.consumer:
                                # print json.loads(message.value)
                                yield message
                except KeyboardInterrupt, e:
                        print e

def doSetDebug(sign,level,func,obj):
        debugLevel = debugDict.get(sign)
        levelNum = levelDict.get(level.upper(),2)
        if levelNum >= debugLevel:
                topic = debugTopicDict.get(sign)
                params = getLogInfo(level,func,obj)
                if 0:
                        producer = Kafka_producer(kafkaHost, kafkaPort, topic)
                        if params:
                                producer.sendjsondata(params)
                else:
                        producer = Kafka_client(brokers, topic)
                        if params:
                                print params
                                producer.sendmsg(params)
#10M
def getFileSize(file):
        file = unicode(file,'utf8')
        fsize = os.path.getsize(file)
        fsize = fsize/float(1024*1024)
        fsize = int(fsize)
        if fsize > 10:
                return 1
        else:
                return 0

def getFileTime(file):
        file = unicode(file,'utf8')
        ftime = os.path.getmtime(file)
        return int(ftime)

def del_file(file):
        obj = open(file,"w+")
        obj.truncate()
        obj.close()

def selectFile(file1,file2):
        err1 = getFileSize(file1)
        err2 = getFileSize(file2)

        if err1 == 0:
                return file1
        elif err2 == 0:
                return file2
        else:
                ftime1 = getFileTime(file1)
                ftime2 = getFileTime(file2)
                if ftime1 > ftime2:
                        del_file(file2)
                        return file2
                else:
                        del_file(file1)
                        return file1

def doGetDebug(sign):
        topic = debugTopicDict.get(sign)
        groupId = debugGroupIdDict.get(sign)
        fileName = debugFileDict.get(sign)
        oneFileName = fileName + str(0)
        twoFileName = fileName + str(1)
        try:
                os.mknod(oneFileName)
                os.chmod(oneFileName, stat.S_IRWXU|stat.S_IRWXG|stat.S_IRWXO)
        except OSError:
                print "%s is exist" % oneFileName
        try:
                os.mknod(twoFileName)
                os.chmod(twoFileName, stat.S_IRWXU|stat.S_IRWXG|stat.S_IRWXO)
        except OSError:
                print "%s is exist." % twoFileName
        #consumer = Kafka_consumer(kafkaIp, kafkaPort, topic, groupId)
        consumer = Kafka_consumer(kafkaHost, kafkaPort, topic, groupId)
        message = consumer.consume_data()

        i = 100
        for mes in message:
                #print i
                if i == 100:
                        file = selectFile(oneFileName,twoFileName)
                objFile = open(file,"a")
                #print mes.value
                info = str(mes.value) + '\n'
                objFile.write(info)
                objFile.close()
                i = i - 1
                if i == 0:
                        i = 100
'''
def main():
        #测试consumer和producer
        #:return:
        doSetDebug("IDS","INFO","idstest","test")
        #doGetDebug("IDS")
        ##测试生产模块
        #producer = Kafka_producer("192.168.1.103", 9092, "test")
        #for id in range(10):
                #params = '{abetst}:{null}---'+str(id)
                #producer.sendjsondata(params)
        ##测试消费模块
        #消费模块的返回格式为ConsumerRecord(topic=u'ranktest', partition=0, offset=202, timestamp=None, 
        #\timestamp_type=None, key=None, value='"{abetst}:{null}---0"', checksum=-1868164195, 
        #\serialized_key_size=-1, serialized_value_size=21)
        #consumer = Kafka_consumer('192.168.1.103', 9092, "test", 'test-python-ranktest')
        #message = consumer.consume_data()
        #for i in message:
                #print i.value


if __name__ == '__main__':
        main()
'''