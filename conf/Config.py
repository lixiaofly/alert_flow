#coding:utf-8

# CRITICAL > ERROR > WARNING > INFO > DEBUG > NOTEST
IDSDEBUG = "WARNING"
VDSDEBUG = "WARNING"
WAFDEBUG = "WARNING"
FLOWDEBUG = "WARNING"

SCANDEBUG = "WARNING"

DBCHAR = "utf8"

KAFKAPORT = 9092
ETCDPORT = 2379

IDSDEBUGTOPIC = "ids-log"
VDSDEBUGTOPIC = "vds-log"
WAFDEBUGTOPIC = "waf-log"
FLOWDEBUGTOPIC = "flow-log"
SCANDEBUGTOPIC = "scan-log"

IDSTOPIC = "ids-alert"
VDSTOPIC = "vds-alert"
WAFTOPIC = "waf-alert"
XDRTOPIC = "xdr"
HTTPTOPIC = "xdrHttp"
FILETOPIC = "xdrFile"
HTTPSTOPIC = "xdr-ssl"
SCANTOPIC = "xdr"

PORTLIST = 80,443,6,17

SCANINTERVAL = 30
IDSPARTITION = 1
VDSPARTITION = 1
WAFPARTITION = 1
FLOWPARTITION = 2
SCANPARTITION = 2
KAFKAHOST = "node1.apt.com,node2.apt.com,node3.apt.com"
BROKERS = "node1.apt.com:9092,node2.apt.com:9092,node3.apt.com:9092"
ZOOKEEPER = "node1.apt.com:2181"
DBHOST = "node2.apt.com"
DBPORT = 3306
DBUSER = "root"
DBPWD = "mysqladmin"
DBNAME = "aptwebservice"
ETCDHOSTS = "node3.apt.com,node2.apt.com,node1.apt.com"
