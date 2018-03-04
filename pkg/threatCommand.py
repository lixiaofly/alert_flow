# -*- coding: UTF-8 -*-
import sys,os
CUR_PATH = os.path.split(os.path.realpath(sys.argv[0]))[0]
sys.path.append("%s/../pkg" % CUR_PATH)
reload(sys)
sys.setdefaultencoding('utf-8')

import Config
import time
from docommand import *

#
# check ip is intranet
#
def ip_into_int(ip):
        # (((((192 * 256) + 168) * 256) + 1) * 256) + 13
        return reduce(lambda x,y:(x<<8)+y,map(int,ip.split('.')))

def is_internal_ip(ip):
        ip = ip_into_int(ip)
        net_a = ip_into_int('10.255.255.255') >> 24
        net_b = ip_into_int('172.31.255.255') >> 20
        net_c = ip_into_int('192.168.255.255') >> 16
        return ip >> 24 == net_a or ip >>20 == net_b or ip >> 16 == net_c

#if __name__ == '__main__':
#        ip = '192.168.0.1'
#        print ip, is_internal_ip(ip)
#        ip = '10.2.0.1'
#        print ip, is_internal_ip(ip)
#        ip = '172.11.1.1'
#        print ip, is_internal_ip(ip)
#
# get now time(string) :%Y-%m-%d %H:%M:%S
#
def getNowTime():
        atime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        return str(atime)
#
# get now time
#
def getNowIntTime():
        timeStr = getNowTime()
        timeInt,_,_ = timeStrToInt(timeStr)
        return timeInt
#
# update starttime and endtime
#
def updateTime(time):
        global startTime, endTime, scanInterval
        nowtime = getNowIntTime()
        if time >= endTime and time <= nowtime:
                startTime = time
                endTime = time + scanInterval
