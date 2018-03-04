import sys,os
CUR_PATH = os.path.split(os.path.realpath(sys.argv[0]))[0]
sys.path.append("%s/../pkg" % CUR_PATH)
reload(sys)
sys.setdefaultencoding('utf-8')

from multiprocessing.dummy import Pool as ThreadPool

from doDebug import doGetDebug

if __name__ == "__main__":
        print "Parent thread %s." % os.getpid()
        nameList = ["IDS","VDS","WAF","FLOW","SCAN"]
        p = ThreadPool(5)
        #for name in nameList:
                #p.apply_async(doGetDebug,args=(name,))
        p.map(doGetDebug,nameList)
        print "Waiting for all thread done..."

        p.close()
        p.join()
        #doGetDebug(LOGTYPE)

        #doGetDebug("IDS")
        #doGetDebug("VDS")
        #doGetDebug("WAF")
        #doGetDebug("FLOW")

