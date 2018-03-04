import json

#person = {"male":{"name":"Shawn"}, "female":{"name":"Betty","age":23},"children":{"name":{"firstName":"Li", "lastName":{"old":"Hong","now":"Ming"}},"age":4}}


#print person

def list_all_dict(sign,dict_a):
        for x in range(len(dict_a)):
                temp_key = dict_a.keys()[x]
                temp_value = dict_a[temp_key]

        if sign == "":
                k = str(temp_key)
        else:
                k = str(sign) + "___" + str(temp_key)

        if not isinstance(temp_value, dict):
                output[k] = temp_value
        else:
                list_all_dict(k,temp_value)

if __name__ == "__main__":
        person = '{"App":{"File":"/tmp//smallfilefile/201794/17/1504493476-6-0-455516","FileLocation":{"File":"/smallfilefile/201794/17/1504493476-6","Offset":0,"Signature":"3bad8346a111a62817fc6d489bb77e768aa48dc45ee8b62b39862acc36047e1","Size":455516}},"Class":103,"Conn":{"Dip":"11.75.222.23","Dport":80,"Proto":6,"Sip":"101.101.78.89","Sport":28647},"ConnEx":{"Dir":false,"Over":false},"ConnSt":{"FlowDown":1847,"FlowUp":1301,"IpFragDown":0,"IpFragUp":0,"PktDown":3,"PktUp":1},"ConnTime":{"End":1504493459000000,"Start":1504493459000000},"Dns":{"PktValid":false},"Ftp":{},"Http":{"Browser":0,"ContentLen":1394,"ContentType":"text/html","Cookie":"","FirstResponseTime":1504493459000000,"HeadFlag":true,"Host":"pos.baidu.com","LastContentTime":1504493459000000,"Location":"","Method":6,"Portal":0,"Refer":"","Request":"","RequestFlag":true,"RequestLocation":{"File":"","Offset":0,"Signature":"","Size":0},"RequestTime":1504493459000000,"Response":"","ResponseLocation":{"File":"","Offset":0,"Signature":"","Size":0},"ServFlag":0,"ServTime":0,"StateCode":200,"Url":"pos.baidu.com/wh/o.htm?ltr=","UserAgent":"","Version":3,"XonlineHost":""},"Id":0,"Ipv4":false,"Mail":{},"Offline_Tag":"","Proxy":{},"QQ":{},"Rtsp":{"AudeoStreamCount":0,"ClientBeginPort":0,"ClientEndPort":0,"ResDelay":0,"ServerBeginPort":0,"ServerEndPort":0,"ServerIp":"","Url":"","UserAgent":"","VideoStreamCount":0},"ServSt":{"FlowDown":0,"FlowUp":0,"IpFragDown":0,"IpFragUp":0,"PktDown":0,"PktUp":0,"TcpDisorderDown":0,"TcpDisorderUp":0,"TcpRetranDown":0,"TcpRetranUp":0},"Sip":{"Bye":false,"CallDir":0,"CallType":0,"CalledNo":"","CallingNo":"","HangupReason":0,"Invite":false,"Malloc":false,"SessionId":"","SignalType":0,"StreamCount":0},"Ssl":{"Client":{"Verfy":false},"Server":{"Verfy":false}},"Task_Id":"","Tcp":{"AckCount":0,"AckDelay":0,"Close":0,"CloseReason":0,"DisorderDown":0,"DisorderUp":0,"FirstRequestDelay":1061,"FirstResponseDely":0,"Handshake12":true,"Handshake23":true,"Mss":1440,"Open":0,"ReportFlag":1,"RetranDown":0,"RetranUp":0,"SessionOK":false,"SynAckCount":0,"SynAckDelay":0,"SynCount":0,"Window":258},"Time":1504493459000000,"Type":1,"Vendor":"","Vpn":{},"Alert":{"Threatname":"Virus.DOS.Vecna.301","Subfile":"/smallfilefile/201794/17/1504493476-6-0-455516//viruscol2/unknown/3460.vir//Com2Exe","Local_threatname":"virus.Windows.Vecna.High","Local_vtype":"virus","Local_platfrom":"Windows","Local_vname":"Vecna","Local_extent":"High","Local_enginetype":"kaspersky","Local_logtype":"VDS","Local_time":1504493645,"Local_engineip":"192.168.1.201"}}'

        person = json.loads(person)
        output = dict()
        if isinstance(person,dict):
                list_all_dict("",person)

        print "output=", output

        print output.keys()
        print output.values()


