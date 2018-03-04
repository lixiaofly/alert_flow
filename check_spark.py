# -*- coding: UTF-8 -*-
import sys,os
CUR_PATH = os.path.split(os.path.realpath(sys.argv[0]))[0]
sys.path.append("%s/conf" % CUR_PATH)
reload(sys)
sys.setdefaultencoding('utf-8')

import MySQLdb 
import Config
import time
import commands
import getopt

waf_offset = 0
#
# get max(id) from mysql table
#
def checkMysql(tablename):
        try:
                db = MySQLdb.connect(Config.DBHOST,Config.DBUSER,Config.DBPWD,Config.DBNAME)
                cursor = db.cursor()
                cursor.execute("SELECT max(id) from %s" % tablename)
                data = cursor.fetchone()
                if data:
                        offset = data[0]
                        if offset == None:
                                offset = 0
        except BaseException,err:
                print str(err)
        finally:
                db.close()
        return offset
#
# get kafka max offset
#
def getKafkaOffset(tablename):
        names = globals()
        ret = 1
        try:
                status,output = commands.getstatusoutput("kafka-run-class kafka.tools.GetOffsetShell \
                                                         --broker-list %s \
                                                         -topic %s \
                                                         --time -1 2>/dev/null" % (Config.BROKERS, tablename))
                if status == 0:
                        for info in output.split('\n'):
                                a = info.split(':')
                                if len(a) == 3:
                                        name = str(a[0]) + str(a[1])
                                        offset = a[2]
                                        names[name] = a[2]
                                        print name, offset
                                        oldname = "old" + name
                                        try:
                                               if names[oldname] != offset:
                                                       ret = 0
                                                       names[oldname] = offset
                                        except KeyError:
                                                names[oldname] = offset
                                else:
                                        continue
                else:
                        ret = 2
        except BaseException,err:
                print str(err)
        finally:
                return ret
#
# check mysql max(id)
#
def check_mysql(tablename):
        global waf_offset
        
        offset = checkMysql(tablename)
        err = 0
        print "check_mysql offset:", offset, "waf_offset:", waf_offset
        if not offset:
                waf_offset = 0
                err = 1
        elif offset != waf_offset:
                waf_offset = offset
        else:
                err = 1

        return err
#
# do getKafkaOffset
#
def check_kafka(topic):
        ret = 1
        for top in topic:
                err = getKafkaOffset(top)
                if err == 0:
                        ret = 0
        return ret

def init(topic, tablename):
        global waf_offset
        for top in topic:
                getKafkaOffset(top)
        waf_offset = checkMysql(tablename) 

def main(topic, tablename, name):
        init(topic, tablename)
        print "init waf_offset = ", waf_offset
        
        path = "%s/.." % (CUR_PATH)

        err_alert = 0
        while True:
                ret = check_kafka(topic)
                print "check_kafka ret:", ret
		
                time.sleep(60)
                if ret == 0:
                        i = 0
                        while i < 3:
                                err = check_mysql(tablename)
                                print "check_mysql err:", err, i, reboot_err
                                if err == 1:
                                        i = i + 1
                                        if i == 3:
                                                print "reboot spark ..."
                                                output = commands.getoutput("%s/spark_stop.sh \
                                                                       %s/alert_flow %s" % (path, path, name))
                                                print output
                                        else:
                                                time.sleep(10)
                                else:
                                        break
                                        
                elif ret == 2:
                        err_alert = err_alert + 1
                        if err_alert == 3:
                                sys.exit(2)


def checkInput(argv):
        try:
                opts, args = getopt.getopt(argv[1:], 'hi:', ['input='])
        except getopt.GetoptError:
                sys.exit(2)
                
        for o, a in opts:
                if o in ('-h', '--help'):
                        sys.exit(0)
                elif o in ('-i', '--input'):
                        if a == "waf":
                                topic = ["waf-alert"]
                                tablename = "alert_waf"
                                name = "kafka_waf.py"
                        elif a == "vds":
                                topic = ["vds-alert"]
                                tablename = "alert_vds"
                                name = "kafka_vds.py"
                        elif a == "ids":
                                topic = ["ids-alert"]
                                tablename = "alert_ids"
                                name = "kafka_ids.py"
                        elif a == "flow":
                                topic = ["xdr", "xdrHttp", "xdrFile"]
                                tablename = "netflow"
                                name = "kafka_netflow.py"
                        elif a == "scan":
                                topic = ["xdr", "xdrHttp", "xdrFile"]
                                tablename = "alert_portscan"
                                name = "portscan.py"
                        else:
                                sys.exit(2)
                        return topic, tablename, name
 
if __name__ == "__main__":
        topic, tablename, name = checkInput(sys.argv)
        print "topic=", topic
        print "tablename=", tablename
        main(topic, tablename, name)

