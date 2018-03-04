# -*- coding: UTF-8 -*-
import sys,os
CUR_PATH = os.path.split(os.path.realpath(sys.argv[0]))[0]
sys.path.append("%s/../conf" % CUR_PATH)
reload(sys)
sys.setdefaultencoding('utf-8')
import time
import commands
import Config
import json

# get configution
#import os
#confFile = os.path.split(os.path.realpath(__file__))[0] + '/../conf/api.conf'
#print confFile

#import ConfigParser 
#cp = ConfigParser.SafeConfigParser()
#cp.read("/home/api.conf")

#import json

idsTopic = Config.IDSTOPIC
vdsTopic = Config.VDSTOPIC
wafTopic = Config.WAFTOPIC
xdrTopic = Config.XDRTOPIC
httpTopic = Config.HTTPTOPIC
fileTopic = Config.FILETOPIC
portScanTopic = Config.SCANTOPIC

idsPartition = Config.IDSPARTITION
vdsPartition = Config.VDSPARTITION
wafPartition = Config.WAFPARTITION
flowPartition = Config.FLOWPARTITION
portScanPartition = Config.SCANPARTITION

brokers = Config.BROKERS
kafkaHost = Config.KAFKAHOST
kafkaPort = Config.KAFKAPORT
portlist = Config.PORTLIST
zook = Config.ZOOKEEPER

idsDebugTopic = Config.IDSDEBUGTOPIC
vdsDebugTopic = Config.VDSDEBUGTOPIC
wafDebugTopic = Config.WAFDEBUGTOPIC
flowDebugTopic = Config.FLOWDEBUGTOPIC
scanDebugTopic = Config.SCANDEBUGTOPIC

IDSdebug = Config.IDSDEBUG.upper()
VDSdebug = Config.VDSDEBUG.upper()
WAFdebug = Config.WAFDEBUG.upper()
FLOWdebug = Config.FLOWDEBUG.upper()
scanDebug = Config.SCANDEBUG.upper()

levelDict = {"CRITICAL":5,"ERROR":4,"WARNING":3,"INFO":2,"DEBUG":1,"NOTEST":0}

idsDebug = levelDict.get(IDSdebug,2)
vdsDebug = levelDict.get(VDSdebug,2)
wafDebug = levelDict.get(WAFdebug,2)
flowDebug = levelDict.get(FLOWdebug,2)
scanDebug = levelDict.get(scanDebug,2)

startTime = 0
endTime = 0

semiConnList = []
semiTimeNum = 1
semiCount = 0
semiAlertSign = 1

allConnList = []
allTimeNum = 0
allCount = 0
allAlertSign = 10

scanInterval = Config.SCANINTERVAL

directList = ['up','down']
protoList = ['tcp','udp']
protocolDict = {6:'tcp'}
boolDict = {'false': 0, 'true':1}
idsOKKeyList = ["Attack_type","Engine","Src_port","Byzoro_type","Proto","Src_ip",\
                "Details","Time","Dest_port","Severity","Dest_ip",\
                'Country','Province','Lng','Lat']
vdsOKKeyList = ["Local_extent","Local_vtype","Local_engineip","Local_threatname","Local_platfrom",\
                "Local_enginetype","Local_vname","Local_logtype","Threatname","log_time",\
                "sourceip","destip","sourceport","destport","app_file",\
                "http_url","Local_subfile","src_country","src_province","src_city",\
                "src_latitude","src_longitude","dest_country","dest_province","dest_city",\
                "dest_latitude","dest_longitude","proto"]
wafOKKeyList = ["Severity","Tags","Hostname","Rev","Uri",\
                "Maturity","Attack","Version","Msg","Client",\
                "Ref","Unique_id","Accuracy","Rule_Data","Rule_Line",\
                "Rule_Ver","Rule_Id","Rule_File","time","src_country",\
                "src_province","src_city","src_latitude","src_longitude","dest_country",\
                "dest_province","dest_city","dest_latitude","dest_longitude","request",\
                "response"]
xdrOKKeyList = ["id","vendor","xdr_id","ipv4","class",\
                "type","time","conn_proto","conn_sport","conn_dport",\
                "conn_sip","conn_dip","cex_over","cex_dir","cst_flup",\
                "cst_fld","cst_pktup","cst_pktd","cst_ipfragup","cst_ipfragd",\
                "ctime_start","ctime_end","sst_flup","sst_fld","sst_pktup",\
                "sst_pktd","sst_ipfragup","sst_ipfragd","sst_tcpdsodup","sst_tcpdsodd",\
                "sst_tcpretrup","sst_tcpretrd","tcp_dsodup","tcp_dsodd","tcp_retranup",\
                "tcp_retrand","tcp_synackdly","tcp_ackdelay","tcp_rportflag","tcp_clsresn",\
                "tcp_fstreqdly","tcp_fstrepdly","tcp_window","tcp_mss","tcp_syncount",\
                "tcp_synackcont","tcp_ackcount","tcp_sesionok","tcp_hndshk12","tcp_hndshk23",\
                "tcp_open","tcp_close","http_host","http_url","http_xolhost",\
                "http_usragnt","http_cnttype","http_refer","http_cookie","http_loction",\
                "http_request","http_reqlfile","http_reqlofst","http_reqlsize","http_reqlsgnt",\
                "http_response","http_rsplfile","http_rsplofst","http_rsplsize","http_rsplsgnt",\
                "http_reqtime","http_fstrsptm","http_lstcnttm","http_servtime","http_cntntlen",\
                "http_statcode","http_method","http_version","http_headflag","http_servflag",\
                "http_reqflag","http_browser","http_portal","sip_callingno","sip_calledno",\
                "sip_sessionid","sip_calldir","sip_calltype","sip_hngupresn","sip_signaltype",\
                "sip_strmcount","sip_malloc","sip_bye","sip_invite","rtsp_Url",\
                "rtsp_usragent","rtsp_serverip","rtsp_clibgnport","rtsp_cliendport","rtsp_servbgnport",\
                "rtsp_servendport","rtsp_vdostrmcnt","rtsp_adostrmcnt","rtsp_resdelay","ftp_state",\
                "ftp_usrcount","ftp_currentdir","ftp_transmode","ftp_transtype","ftp_filecount",\
                "ftp_filesize","ftp_rsptm","ftp_transtm","mail_msgtype","mail_rspstate",\
                "mail_usrname","mail_recvinfo","mail_len","mail_domninfo","mail_recvacont",\
                "mail_hdr","mail_acstype","dns_domain","dns_ipcount","dns_ipver",\
                "dns_ip","dns_ips","dns_rspcode","dns_rspcount","dns_rsprcdcnt",\
                "dns_authcntcnt","dns_xtrrcdcnt","dns_rspdelay","dns_pktvalid","vpn_type",\
                "proxy_type","qq_num","app_protoinf","app_status","app_classid",\
                "app_proto","app_file","app_flocfile","app_flocofst","app_flocsize",\
                "app_flocsgnt","ssl_failresn","serv_vfy","serv_vfyflddsc","serv_vfyfldidx",\
                "scert_ver","scert_srlnum","scert_nbef","scert_naft","scert_kusg",\
                "scert_cntrnam","scert_ognznam","scert_ognzunam","scert_comnnam","sc_floc_dbnam",\
                "sc_floc_tbnam","sc_floc_sgnt","cli_vfy","cli_vfyflddsc","cli_vfyfldidx",\
                "ccert_ver","ccert_srlnum","ccert_nbef","ccert_naft","ccert_kusg",\
                "ccert_cntrnam","ccert_ognznam","ccert_ognzunam","ccert_comnnam","cc_floc_dbnam",\
                "cc_floc_tbnam","cc_floc_sgnt","alert_type","alert_id","xdr_details","offline_tag","task_id"]

XdrKeyDict = {"Id":"id",\
           "Vendor":"vendor",\
           "Xdr_Id":"xdr_id",\
           "Ipv4":"ipv4",\
           "Class":"class",\
           "Type":"type",\
           "Time":"time",\
           "Conn__Proto":"conn_proto",\
           "Conn__Sport":"conn_sport",\
           "Conn__Dport":"conn_dport",\
           "Conn__Sip":"conn_sip",\
           "Conn__Dip":"conn_dip",\
           "ConnEx__Over":"cex_over",\
           "ConnEx__Dir":"cex_dir",\
           "ConnSt__FlowUp":"cst_flup",\
           "ConnSt__FlowDown":"cst_fld",\
           "ConnSt__PktUp":"cst_pktup",\
           "ConnSt__PktDown":"cst_pktd",\
           "ConnSt__IpFragUp":"cst_ipfragup",\
           "ConnSt__IpFragDown":"cst_ipfragd",\
           "ConnTime__Start":"ctime_start",\
           "ConnTime_End":"ctime_end",\
           "ServSt__FlowUp":"sst_flup",\
           "ServSt__FlowDown":"sst_fld",\
           "ServSt__PktUp":"sst_pktup",\
           "ServSt__PktDown":"sst_pktd",\
           "ServSt__IpFragUp":"sst_ipfragup",\
           "ServSt__IpFragDown":"sst_ipfragd",\
           "ServSt__TcpDisorderUp":"sst_tcpdsodup",\
           "ServSt__TcpDisorderDown":"sst_tcpdsodd",\
           "ServSt__TcpRetranUp":"sst_tcpretrup",\
           "ServSt__TcpRetranDown":"sst_tcpretrd",\
           "Tcp__DisorderUp":"tcp_dsodup",\
           "Tcp__DisorderDown":"tcp_dsodd",\
           "Tcp__RetranUp":"tcp_retranup",\
           "Tcp__RetranDown":"tcp_retrand",\
           "Tcp__SynAckDelay":"tcp_synackdly",\
           "Tcp__AckDelay":"tcp_ackdelay",\
           "Tcp__ReportFlag":"tcp_rportflag",\
           "Tcp__CloseReason":"tcp_clsresn",\
           "Tcp__FirstRequestDelay":"tcp_fstreqdly",\
           "Tcp__FirstResponseDelay":"tcp_fstrepdly",\
           "Tcp__Window":"tcp_window",\
           "Tcp__Mss":"tcp_mss",\
           "Tcp__SynCount":"tcp_syncount",\
           "Tcp__SynAckCount":"tcp_synackcont",\
           "Tcp__AckCount":"tcp_ackcount",\
           "Tcp__SessionOK":"tcp_sesionok",\
           "Tcp__Handshake12":"tcp_hndshk12",\
           "Tcp__Handshake23":"tcp_hndshk23",\
           "Tcp__Open":"tcp_open",\
           "Tcp__Close":"tcp_close",\
           "Http__Host":"http_host",\
           "Http__Url":"http_url",\
           "Http__XonlineHost":"http_xolhost",\
           "Http__UserAgent":"http_usragnt",\
           "Http__ContentType":"http_cnttype",\
           "Http__Refer":"http_refer",\
           "Http__Cookie":"http_cookie",\
           "Http__Location":"http_loction",\
           "Http__Request":"http_request",\
           "Http__RequestLocation__File":"http_reqlfile",\
           "Http__RequestLocation__Offset":"http_reqlofst",\
           "Http__RequestLocation__Size":"http_reqlsize",\
           "Http__RequestLocation__Signature":"http_reqlsgnt",\
           "Http__Response":"http_response",\
           "Http__ResponseLocation__File":"http_rsplfile",\
           "Http__ResponseLocation__Offset":"http_rsplofst",\
           "Http__ResponseLocation__Size":"http_rsplsize",\
           "Http__ResponseLocation__Signature":"http_rsplsgnt",\
           "Http__RequestTime":"http_reqtime",\
           "Http__FirstResponseTime":"http_fstrsptm",\
           "Http__LastContentTime":"http_lstcnttm",\
           "Http__ServTime":"http_servtime",\
           "Http__ContentLen":"http_cntntlen",\
           "Http__StateCode":"http_statcode",\
           "Http__Method":"http_method",\
           "Http__Version":"http_version",\
           "Http__HeadFlag":"http_headflag",\
           "Http__ServFlag":"http_servflag",\
           "Http__RequestFlag":"http_reqflag",\
           "Http__Browser":"http_browser",\
           "Http__Portal":"http_portal",\
           "Sip__CallingNo":"sip_callingno",\
           "Sip__CalledNo":"sip_calledno",\
           "Sip__SessionId":"sip_sessionid",\
           "Sip__CallDir":"sip_calldir",\
           "Sip__CallType":"sip_calltype",\
           "Sip__HangupReason":"sip_hngupresn",\
           "Sip__SignalType":"sip_signaltype",\
           "Sip__StreamCount":"sip_strmcount",\
           "Sip__Malloc":"sip_malloc",\
           "Sip__Bye":"sip_bye",\
           "Sip__Invite":"sip_invite",\
           "Rtsp__Url":"rtsp_Url",\
           "Rtsp__UserAgent":"rtsp_usragent",\
           "Rtsp__ServerIp":"rtsp_serverip",\
           "Rtsp__ClientBeginPort":"rtsp_clibgnport",\
           "Rtsp__ClientEndPort":"rtsp_cliendport",\
           "Rtsp__ServerBeginPort":"rtsp_servbgnport",\
           "Rtsp__ServerEndPort":"rtsp_servendport",\
           "Rtsp__VideoStreamCount":"rtsp_vdostrmcnt",\
           "Rtsp__AudeoStreamCount":"rtsp_adostrmcnt",\
           "Rtsp__ResDelay":"rtsp_resdelay",\
           "Ftp__State":"ftp_state",\
           "Ftp__UserCount":"ftp_usrcount",\
           "Ftp__CurrentDir":"ftp_currentdir",\
           "Ftp__TransMode":"ftp_transmode",\
           "Ftp__TransType":"ftp_transtype",\
           "Ftp__FileCount":"ftp_filecount",\
           "Ftp__FileSize":"ftp_filesize",\
           "Ftp__RspTm":"ftp_rsptm",\
           "Ftp__TransTm":"ftp_transtm",\
           "Mail__MsgType":"mail_msgtype",\
           "Mail__RspState":"mail_rspstate",\
           "Mail__UserName":"mail_usrname",\
           "Mail__RecverInfo":"mail_recvinfo",\
           "Mail__Len":"mail_len",\
           "Mail__DomainInfo":"mail_domninfo",\
           "Mail__RecvAccount":"mail_recvacont",\
           "Mail__Hdr":"mail_hdr",\
           "Mail__AcsType":"mail_acstype",\
           "Dns__Domain":"dns_domain",\
           "Dns__IpCount":"dns_ipcount",\
           "Dns__IpVersion":"dns_ipver",\
           "Dns__Ip":"dns_ip",\
           "Dns__Ips":"dns_ips",\
           "Dns__RspCode":"dns_rspcode",\
           "Dns__ReqCount":"dns_rspcount",\
           "Dns__RspRecordCount":"dns_rsprcdcnt",\
           "Dns__AuthCnttCount":"dns_authcntcnt",\
           "Dns__ExtraRecordCount":"dns_xtrrcdcnt",\
           "Dns__RspDelay":"dns_rspdelay",\
           "Dns__PktValid":"dns_pktvalid",\
           "Vpn__Type":"vpn_type",\
           "Proxy__Type":"proxy_type",\
           "QQ__Number":"qq_num",\
           "App__ProtoInfo":"app_protoinf",\
           "App__Status":"app_status",\
           "App__ClassId":"app_classid",\
           "App__Proto":"app_proto",\
           "App__File":"app_file",\
           "App__FileLocation__File":"app_flocfile",\
           "App__FileLocation__Offset":"app_flocofst",\
           "App__FileLocation__Size":"app_flocsize",\
           "App__FileLocation__Signature":"app_flocsgnt",\
           "Ssl__FailReason":"ssl_failresn",\
           "Ssl__Server__Verfy":"serv_vfy",\
           "Ssl__Server__VerfyFailedDesc":"serv_vfyflddsc",\
           "Ssl__Server__VerfyFailedIdx":"serv_vfyfldidx",\
           "Ssl__Server__Cert__Version":"scert_ver",\
           "Ssl__Server__Cert__SerialNumber":"scert_srlnum",\
           "Ssl__Server__Cert__NotBefore":"scert_nbef",\
           "Ssl__Server__Cert__NotAfter":"scert_naft",\
           "Ssl__Server__Cert__KeyUsage":"scert_kusg",\
           "Ssl__Server__Cert__CountryName":"scert_cntrnam",\
           "Ssl__Server__Cert__OrganizationName":"scert_ognznam",\
           "Ssl__Server__Cert__OrganizationUnitName":"scert_ognzunam",\
           "Ssl__Server__Cert__CommonName":"scert_comnnam",\
           "Ssl__Server__Cert__FileLocation__DbName":"sc_floc_dbnam",\
           "Ssl__Server__Cert__FileLocation__TableName":"sc_floc_tbnam",\
           "Ssl__Server__Cert__FileLocation__Signature":"sc_floc_sgnt",\
           "Ssl__Client__Verfy":"cli_vfy",\
           "Ssl__Client__VerfyFailedDesc":"cli_vfyflddsc",\
           "Ssl__Client__VerfyFailedIdx":"cli_vfyfldidx",\
           "Ssl__Client_Cert__Version":"ccert_ver",\
           "Ssl__Client_Cert__SerialNumber":"ccert_srlnum",\
           "Ssl__Client_Cert__NotBefore":"ccert_nbef",\
           "Ssl__Client_Cert__NotAfter":"ccert_naft",\
           "Ssl__Client_Cert__KeyUsage":"ccert_kusg",\
           "Ssl__Client_Cert__CountryName":"ccert_cntrnam",\
           "Ssl__Client_Cert__OrganizationName":"ccert_ognznam",\
           "Ssl__Client_Cert__OrganizationUnitName":"ccert_ognzunam",\
           "Ssl__Client_Cert__CommonName":"ccert_comnnam",\
           "Ssl__Client_Cert__FileLocation__DbName":"cc_floc_dbnam",\
           "Ssl__Client_Cert__FileLocation__TableName":"cc_floc_tbnam",\
           "Ssl__Client_Cert__FileLocation__Signature":"cc_floc_sgnt",\
           "Alert_Type":"alert_type",\
           "Alert_Id":"alert_id",\
           "Xdr_Details":"xdr_details",\
           "Offline_Tag":"offline_tag",\
           "Task_Id":"task_id"}


mainKeyList = ["xdr_id","ipv4","class","type","time","vendor","xdr_details","offline_tag","task_id"]
connKeyList = ["conn_sport","conn_dport","conn_proto","conn_sip","conn_dip"]
connSipInfoKeyList = ["conn_scountry","conn_sprovince","conn_sorg","conn_snetwork","conn_slng",\
                      "conn_slat","conn_stimezone","conn_sutc","conn_sregioncode","conn_sphonecode",\
                      "conn_scountrycode","conn_scontinentcode"]
connDipInfoKeyList = ["conn_dcountry","conn_dprovince","conn_dorg","conn_dnetwork","conn_dlng",\
                      "conn_dlat","conn_dtimezone","conn_dutc","conn_dregioncode","conn_dphonecode",\
                      "conn_dcountrycode","conn_dcontinentcode"]
connStKeyList = ["cst_flup","cst_fld","cst_pktup","cst_pktd","cst_ipfragup","cst_ipfragd"]
connTimeKeyList = ["ctime_start","ctime_end"]
connExKeyList = ["cex_over","cex_dir"]
servStKeyList = ["sst_flup","sst_fld","sst_pktup","sst_pktd","sst_ipfragup",\
                 "sst_ipfragd","sst_tcpdsodup","sst_tcpdsodd","sst_tcpretrup","sst_tcpretrd"]
tcpKeyList = ["tcp_dsodup","tcp_dsodd","tcp_retranup","tcp_retrand","tcp_synackdly",\
              "tcp_ackdelay","tcp_rportflag","tcp_clsresn","tcp_fstreqdly","tcp_fstrepdly",\
              "tcp_window","tcp_mss","tcp_syncount","tcp_synackcont","tcp_ackcount",\
              "tcp_sesionok","tcp_hndshk12","tcp_hndshk23","tcp_open","tcp_close"]
httpKeyList = ["http_host","http_url","http_xolhost","http_usragnt","http_cnttype",\
               "http_refer","http_cookie","http_loction","http_request","http_response","http_reqtime","http_fstrsptm",\
               "http_lstcnttm","http_servtime","http_cntntlen","http_statcode","http_method",\
               "http_version","http_headflag","http_servflag","http_reqflag","http_browser",\
               "http_portal"]
httpRequestLocationKeyList = ["http_reqlfile","http_reqlofst","http_reqlsize","http_reqlsgnt"]
httpResponseLocationKeyList = ["http_rsplfile","http_rsplofst","http_rsplsize","http_rsplsgnt"]
httpHostIpInfoKeyList = ["http_country","http_province","http_org","http_network","http_lng",\
                         "http_lat","http_timezone","http_utc","http_regioncode","http_phonecode",\
                         "http_countrycode","http_continentcode"]
sipKeyList = ["sip_callingno","sip_calledno","sip_sessionid","sip_calldir","sip_calltype",\
              "sip_hngupresn","sip_signaltype","sip_strmcount","sip_malloc","sip_bye",\
              "sip_invite"]
rtspKeyList = ["rtsp_Url","rtsp_usragent","rtsp_serverip","rtsp_clibgnport","rtsp_cliendport",\
               "rtsp_servbgnport","rtsp_servendport","rtsp_vdostrmcnt","rtsp_adostrmcnt","rtsp_resdelay"]
ftpKeyList = ["ftp_state","ftp_usrcount","ftp_transmode","ftp_transtype","ftp_filecount",\
              "ftp_filesize","ftp_rsptm","ftp_transtm","ftp_currentdir"]
mailKeyList = ["mail_msgtype","mail_rspstate","mail_len","mail_acstype","mail_usrname",\
               "mail_recvinfo","mail_domninfo","mail_recvacont","mail_hdr"]
dnsKeyList = ["dns_domain","dns_ipcount","dns_ipver","dns_ip","dns_ips",\
              "dns_rspcode","dns_rspcount","dns_rsprcdcnt","dns_authcntcnt","dns_xtrrcdcnt",\
              "dns_rspdelay","dns_pktvalid"]
dnsIpInfoKeyList = ["dns_country","dns_province","dns_org","dns_network","dns_lng",\
                    "dns_lat","dns_timezone","dns_utc","dns_regioncode","dns_phonecode",\
                    "dns_countrycode","dns_continentcode"]
vpnKeyList = ["vpn_type"]
proxyKeyList = ["proxy_type"]
qqKeyList = ["qq_num"]
appKeyList = ["app_protoinf","app_status","app_classid","app_proto","app_file"]
appFileLocationKeyList = ["app_flocfile","app_flocofst","app_flocsize","app_flocsgnt"]
sslKeyList = ["ssl_failresn"]
sslServerKeyList = ["serv_vfy","serv_vfyflddsc","serv_vfyfldidx"]
sslServerCertKeyList = ["scert_ver","scert_srlnum","scert_nbef","scert_naft","scert_kusg",\
                        "scert_cntrnam","scert_ognznam","scert_ognzunam","scert_comnnam"]
sslServerCertFileLocationKeyList = ["sc_floc_dbnam","sc_floc_tbnam","sc_floc_sgnt"]
sslClientKeyList = ["cli_vfy","cli_vfyflddsc","cli_vfyfldidx"]
sslClientCertKeyList = ["ccert_ver","ccert_srlnum","ccert_nbef","ccert_naft","ccert_kusg",\
                        "ccert_cntrnam","ccert_ognznam","ccert_ognzunam","ccert_comnnam"]
sslClientCertFileLocationKeyList = ["cc_floc_dbnam","cc_floc_tbnam","cc_floc_sgnt"]


mainList = ['Id', 'Ipv4', 'Class', 'Type', 'Time', 'Vendor', 'Xdr_Details', 'Offline_Tag', 'Task_Id']
connList = ['Sport', 'Dport', 'Proto', 'Sip', 'Dip']
connSipInfoList = ['Country','Province','Organization','Network','Lng',\
                   'Lat','TimeZone','UTC','RegionalismCode','PhoneCode',\
                   'CountryCode','ContinentCode']
connDipInfoList = ['Country','Province','Organization','Network','Lng',\
                   'Lat','TimeZone','UTC','RegionalismCode','PhoneCode',\
                   'CountryCode','ContinentCode']
connStList = ['FlowUp', 'FlowDown', 'PktUp', 'PktDown', 'IpFragUp', 'IpFragDown']
connTimeList = ['Start', 'End']
connExList = ['Over', 'Dir']
servStList = ['FlowUp','FlowDown','PktUp','PktDown','IpFragUp', \
              'IpFragDown','TcpDisorderUp','TcpDisorderDown','TcpRetranUp','TcpRetranDown']
tcpList = ['DisorderUp','DisorderDown','RetranUp','RetranDown','SynAckDelay', \
           'AckDelay','ReportFlag','CloseReason','FirstRequestDelay','FirstResponseDelay',\
           'Window','Mss','SynCount','SynAckCount','AckCount',\
           'SessionOK','Handshake12','Handshake23','Open','Close']
httpList = ['Host', 'Url', 'XonlineHost', 'UserAgent', 'ContentType', \
            'Refer', 'Cookie', 'Location', 'Request', 'Response', \
            'RequestTime', 'FirstResponseTime', 'LastContentTime','ServTime', 'ContentLen', \
            'StateCode', 'Method', 'Version', 'HeadFlag', 'ServFlag', \
            'RequestFlag', 'Browser', 'Portal']
httpRequestLocationList = ["File","Offset","Size","Signature"]
httpResponseLocationList = ["File","Offset","Size","Signature"]
httpHostIpInfoList = ['Country','Province','Organization','Network','Lng',\
                      'Lat','TimeZone','UTC','RegionalismCode','PhoneCode',\
                      'CountryCode','ContinentCode']
sipList = ['CallingNo', 'CalledNo', 'SessionId', 'CallDir', 'CallType', \
           'HangupReason', 'SignalType', 'StreamCount', 'Malloc', 'Bye', \
           'Invite']
rtspList  = ['Url', 'UserAgent', 'ServerIp', 'ClientBeginPort', 'ClientEndPort', \
         'ServerBeginPort', 'ServerEndPort', 'VideoStreamCount', 'AudeoStreamCount', 'ResDelay']
ftpList = ['State', 'UserCount', 'CurrentDir', 'TransMode', 'TransType', 'FileCount', \
           'FileSize', 'RspTm', 'TransTm']
mailList = ['MsgType', 'RspState','UserName','RecverInfo',  'Len', \
            'DomainInfo', 'RecvAccount', 'Hdr', 'AcsType']
dnsList = ['Domain', 'IpCount', 'IpVersion', 'Ip', 'Ips', \
           'RspCode', 'ReqCount', 'RspRecordCount', 'AuthCnttCount', 'ExtraRecordCount', \
           'RspDelay', 'PktValid']
dnsIpInfoList = ['Country','Province','Organization','Network','Lng',\
                 'Lat','TimeZone','UTC','RegionalismCode','PhoneCode',\
                 'CountryCode','ContinentCode']
vpnList = ['Type']
proxyList = ['Type']
qqList = ['Number']
appList = ['ProtoInfo', 'Status', 'ClassId', 'Proto', 'File']
appFileLocationList = ['File','Offset','Size','Signature']
sslList = ['FailReason']
sslServerList = ['Verfy','VerfyFailedDesc','VerfyFailedIdx']
sslServerCertList = ['Version','SerialNumber','NotBefore','NotAfter','KeyUsage',\
                     'CountryName','OrganizationName','OrganizationUnitName','CommonName']
sslServerCertFileLocationList = ['DbName','TableName','Signature']
sslClientList = ['Verfy','VerfyFailedDesc','VerfyFailedIdx']
sslClientCertList = ['Version','SerialNumber','NotBefore','NotAfter','KeyUsage',\
                     'CountryName','OrganizationName','OrganizationUnitName','CommonName']
sslClientCertFileLocationList = ['DbName','TableName','Signature']


offsetDict = {}

def delNullRDD(obj):
        #if isinstance(obj, dict):
        if obj == None or len(obj) == 0:
                return False
        else:
                return True

#
# get the new offset
#
def getNewOffset(topic,partition):
        status,output = commands.getstatusoutput("sh /home/getOffset.sh %s %d %s 2>/dev/null" %(topic, partition, brokers))
        return output
#
# 0:false, 1:true
#
def changeBoolToInt(inputStr):
        if inputStr == True:
                return 1
        else:
                return 0
#
# time int [64] ws -> s
#
def timeWSToS(timeWS):
        if len(str(timeWS)) > 15:
                return int(str(timeWS)[:-6])
        else:
                return timeWS
#
# change time: string -> int[64]
#
def timeStrToInt(timeStr):
        a = time.strptime(timeStr,'%Y-%m-%d %H:%M:%S')
        return time.mktime(a), time.strftime("%Y%m%d%H", a), time.strftime("%M", a)
#
# change timestamp: int[64] -> string
#
def timeStampToInt(timeStamp):
        stime = int(timeStamp)
        other = int(str(timeStamp)[len(str(timeStamp))-6:])
        a = time.localtime(stime)

        return time.strftime("%Y-%m-%d %H:%M:%S", a), time.strftime("%Y%m%d%H", a), time.strftime("%M", a), other
#
#change timestamp: int[64] -> string[yy:mm:dd:00:00:00]
#
def timeStampToymd(timeStamp):
        stime = int(timeStamp)
        a = time.localtime(stime)
        timeStr = time.strftime("%Y-%m-%d 00:00:00", a)
        b = time.strptime(timeStr,'%Y-%m-%d %H:%M:%S')
        return int(time.mktime(b))
#
#change timestamp: int[64] -> string[yy:mm:dd:00:00:00]
#
def timeStampToymdM(timeStamp):
	if timeStamp:
        	stime = int(timeStamp)
        	a = time.localtime(stime)
        	timeStr = time.strftime("%Y-%m-%d %H:%M:00", a)
        	b = time.strptime(timeStr,'%Y-%m-%d %H:%M:%S')
        	return int(time.mktime(b))
	else:
		return 0
#
# change time '%Y-%m-%d %H:%M:%S' -> '%Y-%m-%d %H:%M:00', -> int[64]
#
def handleSignTime(timeStamp):
        _, ymdh, m, _ = timeStampToInt(int(timeStamp))

        newtime = ymdh + str(int(m)) + '00'
        b = time.strptime(newtime,'%Y%m%d%H%M%S')

        return int(time.mktime(b))
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
        return int(timeInt)
#
# write the debug log
#
def writeLog(argv0, argv1, *tupleArg, **dictArg):
        pass
#
# check obj is global or local ?
#
def isset(obj):
        try:
                type(eval(obj))
        except:
                return 1
        else:
                return 0
#
# tuple -> string
# ('a','b') -> '(a,b)'
#
def changeMysqlKeyTupleToStr(obj):
        if isinstance(obj, tuple):
                key = ",".join(obj)
                newkeys = str('(') + str(key) + str(')')
                return newkeys
        else:
                return ''
#
# tuple to string
# ('a', 'b', 3) -> '('a','b','3')'
#
def changeMysqlValueTupleToStr(obj):
        if isinstance(obj, tuple):
                valuesList = []
                for v in obj:
                        valuesList.append(str(v))
                newvalues = tuple(valuesList)
                value = "\',\'".join(newvalues)
                newvalues = str('(\'') + str(value) + str('\')')
                return newvalues
        else:
                return ''
#
# ["A","B",3] ==> "A__B__3"
#
def handleListToStr(value):
        outStr = ''
        for v in value:
                if v == value[-1]:
                        b = str(v)
                else:
                        b = str(v) + '__'
                outStr += b
        return outStr

def handleXdrJson(inputDict,keyList, mysqlKeyList):
        output = dict()
        for i in range(len(keyList)):
                key = keyList[i]
                mysqlKey = mysqlKeyList[i]
                value = inputDict.get(key,'')
                if value == '':
                        continue
                if isinstance(value, unicode):
                        try:
                                value = str(value)
                        except UnicodeEncodeError:
                                pass
                elif isinstance(value, list):
                        value = handleListToStr(value)
                elif value == False:
                        value = 0
                elif value == True:
                        value = 1
                output[mysqlKey] = value
        return output
#
# ["A","B","C"],["a","b","c'] ==> {"A":"a","B":"b","C":"c"}
#
def listToDict(keyList, valueList):
        return dict(zip(keyList,valueList))
#
# {"A":"a","B":"b"},{"C":"c"} ==> {"A":"a","B":"b","C":"c"}
#
def dictToDict(dict1,dict2):
        return dict(dict1, **dict2)

def getDict(inputDict,key,sign):
        names = globals()

        obj = inputDict.get(key, {})
        return  handleXdrJson(obj, names[sign + "List"], names[sign + "KeyList"])

def getSonDict(obj, sonKey, keyList, mysqlKeyList):
        sonDict = obj.get(sonKey, {})

	return handleXdrJson(sonDict, keyList, mysqlKeyList)
#
# get main dict
#
def getMainDict(xdrDict):
        mainDict = handleXdrJson(xdrDict, mainList , mainKeyList)
        mainDict['time'] = timeWSToS(mainDict.get('time',0))
        return mainDict
#
# get Conn Dict
#
def getConnDict(xdrDict):
        connDict = getDict(xdrDict,"Conn","conn")
        proto = connDict.get('conn_proto',0)
        connDict['conn_proto'] = protocolDict.get(proto,'udp')
        #return obj
        sipInfo = xdrDict.get("Conn",{})
        sipDict = getSonDict(sipInfo,"SipInfo",connSipInfoList,connSipInfoKeyList)
        dipDict = getSonDict(sipInfo,"DipInfo",connDipInfoList,connDipInfoKeyList)
        
        return reduce(dictToDict, [connDict, sipDict, dipDict])
#
# get ConnEx dict
#
def getConnExDict(xdrDict):
        return getDict(xdrDict, "ConnEx","connEx")
#
# get ConnSt dict
#
def getConnStDict(xdrDict):
        return getDict(xdrDict,"ConnSt","connSt")

#
# get ConnTime dict 
#
def getConnTimeDict(xdrDict):
        obj = getDict(xdrDict, "ConnTime","connTime")
        obj["ctime_start"] = timeWSToS(obj.get("ctime_start"))
        obj["ctime_end"] = timeWSToS(obj.get("ctime_end"))
        return obj
#
# get servst dict
#
def getServStDict(xdrDict):
        return getDict(xdrDict, "ServSt", "servSt")
#
# get tcp dict
#
def getTcpDict(xdrDict):
        return getDict(xdrDict, "Tcp", "tcp")
#
# get http dict
#
def getHttpDict(xdrDict):
        httpDict = getDict(xdrDict,"Http","http")
        obj = xdrDict.get("Http",{})
        reqDict = getSonDict(obj,"RequestLocation",httpRequestLocationList,httpRequestLocationKeyList)
        respDict = getSonDict(obj,"ResponseLocation",httpResponseLocationList,httpResponseLocationKeyList)
        ipDict = getSonDict(obj,"HostIpInfo",httpHostIpInfoList,httpHostIpInfoKeyList)
        
        return reduce(dictToDict, [httpDict, reqDict, respDict,ipDict])
#
# get sip dict
#
def getSipDict(xdrDict):
        return getDict(xdrDict,"Sip","sip")
#
# get rtsp dict
#
def getRtspDict(xdrDict):
        return getDict(xdrDict, "Rtsp","rtsp")
#
# get ftp dict
#
def getFtpDict(xdrDict):
        return getDict(xdrDict, "Ftp","ftp")
#
# get mail dict
#
def getMailDict(xdrDict):
        return getDict(xdrDict, "Mail","mail")
#
# get dns dict
#
def getDnsDict(xdrDict):
        dnsDict = getDict(xdrDict, "Dns", "dns")
        obj = xdrDict.get("Dns",{})
        ipDict = getSonDict(obj,"IpInfo",dnsIpInfoList,dnsIpInfoKeyList)
        
        return reduce(dictToDict, [dnsDict, ipDict])
        #return getDict(xdrDict, "Dns", "dns")
#
# get vpn dict
#
def getVpnDict(xdrDict):
        return getDict(xdrDict, "Vpn", "vpn")
#
# get Proxy dict
#
def getProxyDict(xdrDict):
        return getDict(xdrDict, "Proxy","proxy")
#
# get qq dict
#
def getQqDict(xdrDict):
        return getDict(xdrDict, "QQ","qq")
#
# get app dict
#
def getAppDict(xdrDict):
        appDict = getDict(xdrDict, "App","app")
        obj = xdrDict.get("App",{})
        sonDict = getSonDict(obj,"FileLocation",appFileLocationList,appFileLocationKeyList)
        return reduce(dictToDict,[appDict,sonDict])
#
# get ssl dict
#
def getSslDict(xdrDict):
        sslDict = getDict(xdrDict, "Ssl","ssl")
        objSsl = xdrDict.get("ssl",{})
        
        serverDict = getSonDict(objSsl,"Server",sslServerList,sslServerKeyList)
        objServerCert = objSsl.get("Server",{})
        serverCertDict = getSonDict(objServerCert, "Cert",sslServerCertList,sslServerCertKeyList)
        objserverCertFile = objServerCert.get("Cert", {})
        serverCertFileDict = getSonDict(objserverCertFile, "FileLocation",sslServerCertFileLocationList,sslServerCertFileLocationKeyList)
        
        clientDict = getSonDict(objSsl,"Client",sslClientList,sslClientKeyList)
        objClientCert = objSsl.get("Client",{})
        clientCertDict = getSonDict(objClientCert, "Cert",sslClientCertList,sslClientCertKeyList)
        objClientCertFile = objClientCert.get("Cert", {})
        clientCertFileDict = getSonDict(objClientCertFile, "FileLocation",sslClientCertFileLocationList,sslClientCertFileLocationKeyList)
        
        return reduce(dictToDict,[sslDict,serverDict,serverCertDict,serverCertFileDict,clientDict,clientCertDict,clientCertFileDict])
#
# get new xdr json 
#        
def getNewXdr(xdrDict):
        #xdrDict = json.loads(xdrDict)
        
        mainDict = getMainDict(xdrDict)
        connDict = getConnDict(xdrDict)
        connExDict = getConnExDict(xdrDict)
        connStDict = getConnStDict(xdrDict)
        connTimeDict = getConnTimeDict(xdrDict)
        servStDict = getServStDict(xdrDict)
        tcpDict = getTcpDict(xdrDict)
        httpDict = getHttpDict(xdrDict)
        sipDict = getSipDict(xdrDict)
        rtspDict = getRtspDict(xdrDict)
        ftpDict = getFtpDict(xdrDict)
        mailDict = getMailDict(xdrDict)
        dnsDict = getDnsDict(xdrDict)
        vpnDict = getVpnDict(xdrDict)
        proxyDict = getProxyDict(xdrDict)
        qqDict = getQqDict(xdrDict)
        appDict = getAppDict(xdrDict)
        sslDict = getSslDict(xdrDict)
        
        dictList = [mainDict,connDict,connExDict,connStDict,connTimeDict,\
                    servStDict,tcpDict,httpDict,sipDict,rtspDict,\
                    ftpDict,mailDict,dnsDict,vpnDict,proxyDict,\
                    qqDict,appDict,sslDict]
        newDict = reduce(dictToDict,dictList)

        return newDict

def changeXdrRow(xdrDict, keyList, sign):
        newDict = {}
        for k in keyList:
                if len(sign) > 0:
                        key = sign + str("_") + k
                        if key == "App_File":
                                newDict[key] = xdrDict.get(sign,{}).get("FileLocation",{}).get(k,'')
                        else:
                                newDict[key] = xdrDict.get(sign,{}).get(k, '')
                else:
                        key = k
                        newDict[key] = xdrDict.get(k,'')
        #print keyList,newDict
        return newDict

def changeMainDict(mainDict):
        mainDict["Time"] = timeWSToS(mainDict.get('Time',0))
        mainDict['Ipv4'] = changeBoolToInt(mainDict.get('Ipv4',0))
        return mainDict

def changeConnDict(connDict):
        connDict['Conn_Proto'] = protocolDict.get(connDict.get('Conn_Proto','udp'), 'udp')
        return connDict

def changeConnTimeDict(connTimeDict):
        connTimeDict["ConnTime_Start"] = timeWSToS(connTimeDict.get("ConnTime_Start", 0))
        connTimeDict["ConnTime_End"] = timeWSToS(connTimeDict.get("ConnTime_End", 0))
        return connTimeDict

def changeConnExDict(connExDict):
        connExDict["ConnEx_Over"] = changeBoolToInt(connExDict.get("ConnEx_Over", 0))
        connExDict["ConnEx_Dir"] = changeBoolToInt(connExDict.get("ConnEx_Dir",0))
        return connExDict

def changeTcpDict(tcpDict):
        tcpDict["Tcp_SessionOK"] = changeBoolToInt(tcpDict.get("Tcp_SessionOK", 0))
        tcpDict["Tcp_Handshake12"] = changeBoolToInt(tcpDict.get("Tcp_Handshake12", 0))
        tcpDict["Tcp_Handshake23"] = changeBoolToInt(tcpDict.get("Tcp_Handshake23", 0))
        return tcpDict

def changeHttpDict(httpDict):
        httpDict["Http_HeadFlag"] = changeBoolToInt(httpDict.get("Http_HeadFlag", 0))
        httpDict["Http_RequestFlag"] = changeBoolToInt(httpDict.get("Http_RequestFlag", 0))
        return httpDict

def changeSipDict(sipDict):
        sipDict["Sip_Malloc"] = changeBoolToInt(sipDict.get("Sip_Malloc", 0))
        sipDict["Sip_Bye"] = changeBoolToInt(sipDict.get("Sip_Bye", 0))
        sipDict["Sip_Invite"] = changeBoolToInt(sipDict.get("Sip_Invite", 0))
        return sipDict

def changeDnsDict(dnsDict):
        dnsDict["Dns_PktValid"] = changeBoolToInt(dnsDict.get("Dns_PktValid", 0))
        return dnsDict

def getOneRowXdr(xdrDict):
        newXdrDict = {}
        
        mainDict1 = changeXdrRow(xdrDict, mainList, "")
        mainDict = changeMainDict(mainDict1)
        
        connDict1 = changeXdrRow(xdrDict, connList, "Conn")
        connDict = changeConnDict(connDict1)
        
        connStDict = changeXdrRow(xdrDict, connStList, "ConnSt")

        connTimeDict1 = changeXdrRow(xdrDict, connTimeList, "ConnTime")
        connTimeDict = changeConnTimeDict(connTimeDict1)
        
        connExDict1 = changeXdrRow(xdrDict, connExList, "ConnEx")
        connExDict = changeConnExDict(connExDict1)
        
        servStDict = changeXdrRow(xdrDict, servStList, "ServSt")
        
        tcpDict1 = changeXdrRow(xdrDict, tcpList, "Tcp")
        tcpDict = changeTcpDict(tcpDict1)
        
        httpDict1 = changeXdrRow(xdrDict, httpList, "Http")
        httpDict = changeHttpDict(httpDict1)
        
        sipDict1 = changeXdrRow(xdrDict, sipList, "Sip")
        sipDict = changeSipDict(sipDict1)
        
        rtspDict = changeXdrRow(xdrDict, rtspList, "Rtsp")
        ftpDict = changeXdrRow(xdrDict, ftpList, "Ftp")
        mailDict = changeXdrRow(xdrDict, mailList, "Mail")
        
        dnsDict1 = changeXdrRow(xdrDict, dnsList, "Dns")
        dnsDict = changeDnsDict(dnsDict1)
        
        vpnDict = changeXdrRow(xdrDict, vpnList, "Vpn")
        proxyDict = changeXdrRow(xdrDict, proxyList, "Proxy")
        qqDict = changeXdrRow(xdrDict, qqList, "QQ")
        appDict = changeXdrRow(xdrDict, appList, "App")
        
        for a in [mainDict, connDict, connStDict, connTimeDict, connExDict, servStDict, \
                  tcpDict, httpDict, sipDict, rtspDict, ftpDict, \
                  mailDict, dnsDict, vpnDict, proxyDict, qqDict, appDict]:
                newXdrDict = dict(newXdrDict, **a)
        return newXdrDict
#
# change the xdr to one layer structure
# {"A":{"B":"b"}} --> {"A_B":"b"}
#
NEWONEROWXDR = dict()
def list_all_dict(sign,dict_a):
        global NEWONEROWXDR
        for x in range(len(dict_a)):
                temp_key = dict_a.keys()[x]
                temp_value = dict_a[temp_key]

        if sign == "":
                k = str(temp_key)
        else:
                k = str(sign) + "___" + str(temp_key)

        if not isinstance(temp_value, dict):
                NEWONEROWXDR[k] = temp_value
        else:
                list_all_dict(k,temp_value)

def getNewOneRowXdr(obj):
        global NEWONEROWXDR
        NEWONEROWXDR = dict()
        if isinstance(obj,dict):
                list_all_dict("",obj)
        return NEWONEROWXDR

def getNewXdrDict(obj):
        if isinstance(obj, dict) and len(obj) > 0:
                #newXdr = getOneRowXdr(obj)
                newXdr = getNewXdr(obj)
                newXdr['Alert'] = obj['Alert']
                return newXdr
        else:
                return ''
#
# json.loads()
#
def checkJson(obj):
        try:
                err = json.loads(obj)
        except:
                err = ''
        finally:
                return err
def checkXdrDict(rdd):
        newRDD = rdd.map(lambda x:checkJson(x))
        return newRDD
#
# delete checkpoint directory
#                               
def delCheckPoint(checkpointDirectory):
        for i in range(5):
                status,output = commands.getstatusoutput("hadoop fs -rmr %s 2>/dev/null" % checkpointDirectory)
                if status == 0 or status == 256:
                        return 0
        return 1
#
# help info
#
def Usage():
        print 'Usage:'
        print '-h, --help: print help message'
        print '-c, --checkpoint: [yes|no], Whether to start checkpoint'

