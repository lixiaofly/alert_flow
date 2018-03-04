# -*- coding: UTF-8 -*-
###spark streaming&&kafka
#import MySQLdb
import sys,os
CUR_PATH = os.path.split(os.path.realpath(sys.argv[0]))[0]
sys.path.append(CUR_PATH)
reload(sys)
sys.setdefaultencoding('utf-8')
from MySqlConn import Mysql
from docommand import *
#
# get init mysql offset , write to offsetDict{}
#
def getInitOffset():
        global offsetDict
        connMysql = Mysql()
        try:
                sqli = "select name,offset from offset"
                result = connMysql.getAll(sqli)
                if result:
                        for row in result:
                               offsetDict[row["name"]] = row["offset"] 
        except:
                print "getInitOffset error !"
        finally:
                connMysql.dispose()
#
# read offset from mysql
#
def insertOffsetMysql(connMysql, value):
        sqli = "insert into offset(name, offset) values%s"
        connMysql.insertOne(sqli, str(value))
	
def updateOffsetMysql(connMysql, name,offset):
        sqli = "update offset set offset = %s where name = %s"
        connMysql.update(sqli,param=(offset,name))
        
def initOffset(connMysql, sign, topic, partition):
        name = sign + topic + str(partition)

        newoffset = getNewOffset(topic, partition)
        try:
                newoffset = int(newoffset)
        except:
                newoffset = -1
        print "newoffset:", newoffset, type(newoffset)
        offset = offsetDict.get(name, -1)
        print "try-initOffset-offset:", offset
        if offset > newoffset:
                print "offset > newoffset:",offset, newoffset
                offset = newoffset
                updateOffsetMysql(connMysql, name, offset)
        elif  offset == -1:
                print "offset = -1"
                if newoffset == -1:
                        offset = 0
                else:
                        offset = newoffset
                value = (name,offset)
                insertOffsetMysql(connMysql, value)
        return offset
		
def insertXdrMysql(connMysql, keysTuple, valuesTuple, alert_id, tag, alert_type):
        keysList = list(keysTuple)
        keysList.append('alert_id')
        keysList.append('alert_type')
        newKeysTuple = tuple(keysList)

        valuesList = list(valuesTuple)
        valuesList.append(alert_id)
        valuesList.append(alert_type)
        newValuesTuple = tuple(valuesList)

        newkeys = changeMysqlKeyTupleToStr(newKeysTuple)
        newvalues = changeMysqlValueTupleToStr(newValuesTuple)

        if tag == "offline":
                sqli = "insert into xdr_offline%s values%s"
        elif tag == "rule":
                sqli = "insert into xdr_offline_rule%s values%s"
        elif tag == "portscan":
                sqli = "insert into portscan_xdr%s values%s"
        else:
                sqli = "insert into xdr%s values%s"
        result = connMysql.insertOne(sqli,(newkeys, newvalues))
        return result
