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
scan_offset = 0

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
                else:
                        offset = 0
        except BaseException,err:
                print str(err)
        finally:
                db.close()

        return offset

def check_scan():
        global scan_offset
        tablename = "portscan_xdr"
        
        offset = checkMysql(tablename)
        
        if not offset:
                scan_offset = 0
                return 1
        elif offset != scan_offset:
                scan_offset = offset
                return 0
        else:
                return 1

def main():
        err_alert = 0
        path = "%s/.." % (CUR_PATH)
        while True:
                err = check_scan()
                if err == 1:
                        err_alert = err_alert + 1
                        if err_alert == 60:
                                err_alert = 0
                                print "reboot spark ..."
                                output = commands.getoutput("%s/spark_stop.sh \
                                                            %s/alert_flow \
                                                            portscan.py" % (path, path))
                                print output
                else:
                        err_alert = 0
                time.sleep(60)
                
if __name__ == "__main__":
        main()

