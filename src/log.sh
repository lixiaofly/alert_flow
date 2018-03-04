#!/bin/bash

answer()
{
        echo "$@" >>/dev/tty
}

get_file_time()
{
        local file="$1"
        local finfo=$(stat ${file} | grep Modify)
        local fdate=$(echo ${finfo} | awk '{print $2}')
        local ftime=$(echo ${finfo} | awk '{split($3,var,".");print var[1]}')
        #answer "finfo=${finfo}"	
        #answer "fdate=${fdate}"
        #answer "ftime=${ftime}"
	
        echo $(date -d "${fdate} ${ftime}" +%s)
}

get_file_size()
{
        local file="$1"

        echo $(ls -lk ${file} |awk '{print $5}')	
}

select_file()
{
        local file1="$1"
        local file2="$2"

        local s1=$(get_file_size "${file1}")
        local s2=$(get_file_size "${file2}")

        answer "s1=${s1}"
        answer "s2=${s2}"

        if [[ ${s1} -lt 10240 ]];then
                echo ${file1}
        elif [[ ${s2} -lt 10240 ]];then
                echo ${file2}
        else
                local t1=$(get_file_time "${file1}")
                local t2=$(get_file_time "${file2}")

                answer "t1=${t1}"
                answer "t2=${t2}"

                if [[ ${t1} -gt ${t2} ]]; then
                        > "${file2}"
                        echo "${file2}"
                else
                        > "${file1}"
                        echo "${file1}"
                fi
        fi
}

do_log()
{
        local zk="$1"
        local topic="$2"
        local file="$3"; shift 3

        local cmd="kafka-console-consumer"

        eval "${cmd} --zookeeper ${zk} --topic ${topic} >>${file} 2>&1 &"
        local pid=$!
        #answer "do_log=${pid}"
        echo ${pid}
        #answer "do_log=${pid}"
}

do_start()
{
        local sign="$1"
        local zk="$2"
        local topic="$3"; shift 3

        answer "******start ${sign}******"

        answer "sign=${sign}"
        answer "zk=${zk}"
        answer "topic=${topic}"
        case ${sign} in
        "IDS")
                local file1="ids0.log"
                local file2="ids1.log"
                ;;
        "VDS")
                local file1="vds0.log"
                local file2="vds1.log"
                ;;
        "WAF")
                local file1="waf0.log"
                local file2="waf1.log"
                ;;
        "FLOW")
                local file1="flow0.log"
                local file2="flow1.log"
                ;;
        "SCAN")
                local file1="scan0.log"
                local file2="scan1.log"
                ;;
        "*")
                answer "sign ${sign} is not support"
                ;;
        esac

        #answer "file1=${file1},file2=${file2}"
        if [[ ! -f /tmp/${file1} ]];then
                touch /tmp/${file1}
        fi
        if [[ ! -f /tmp/${file2} ]];then
                touch /tmp/${file2}
        fi

        local file=$(select_file "/tmp/${file1}" "/tmp/${file2}")

        answer "file=${file}"

        local log_id=$(do_log "${zk}" "${topic}" "${file}")
        #answer "do_start id"
        echo ${log_id}
        #answer "do_start end"
}

do_kill()
{
        local id=$@;
        #local sub_id=$(ps --ppid ${id}| awk '{if($1~/[0-9]+/) print $1}')
        answer "do_kill id=${id}"
        #answer "do_kill sub_id=${sub_id}"

        #for i in ${sub_id}
        #do
        answer "kill ${id}"
        kill -9 ${id}
        #done
}

main()
{
        local sign="$1"; shift 1
        if [[ "${sign}" == "auto" ]];then
                local conf_file="${APT_PACKAGE}/spark_app/alert_flow/conf/Config.py"
        else
                local conf_file="../conf/Config.py"
        fi

        if [[ ! -f "${conf_file}" ]];then
                answer "config file ${conf_file} is not exist ."
                return 1
        fi

        local zk=$(cat ${conf_file} |grep -v '^#' |grep -w '^ZOOKEEPER' |awk -F '=' '{print $2}')

        local ids_topic=$(cat ${conf_file} |grep -v '^#' |grep -w '^IDSDEBUGTOPIC' |awk -F '=' '{print $2}')
        local vds_topic=$(cat ${conf_file} |grep -v '^#' |grep -w '^VDSDEBUGTOPIC' |awk -F '=' '{print $2}')
        local waf_topic=$(cat ${conf_file} |grep -v '^#' |grep -w '^WAFDEBUGTOPIC' |awk -F '=' '{print $2}')
        local flow_topic=$(cat ${conf_file} |grep -v '^#' |grep -w '^FLOWDEBUGTOPIC' |awk -F '=' '{print $2}')
        local scan_topic=$(cat ${conf_file} |grep -v '^#' |grep -w '^SCANDEBUGTOPIC' |awk -F '=' '{print $2}')

        answer "zk=${zk}"

        local ids_id=0
        local vds_id=0
        local waf_id=0
        local flow_id=0
        local scan_id=0

        local ids_err=0
        local vds_err=0
        local waf_err=0
        local flow_err=0
        local scan_err=0

        while true;do
                yarn application -list |grep -w "kafka_ids.py" >/dev/null 2>&1; ids_err=$?
                yarn application -list |grep -w "kafka_vds.py" >/dev/null 2>&1; vds_err=$?
                yarn application -list |grep -w "kafka_waf.py" >/dev/null 2>&1; waf_err=$?
                yarn application -list |grep -w "kafka_netflow.py" >/dev/null 2>&1; flow_err=$?
                yarn application -list |grep -w "portscan.py" >/dev/null 2>&1; scan_err=$?
                
                if [[ ${ids_err} -eq 0 ]];then
                        ids_id=$(do_start "IDS" "${zk}" "${ids_topic}")
                        answer "ids_id=${ids_id}"
                fi
                if [[ ${vds_err} -eq 0 ]];then
                        vds_id=$(do_start "VDS" "${zk}" "${vds_topic}")
                        answer "vds_id=${vds_id}"
                fi
                if [[ ${waf_err} -eq 0 ]];then
                        waf_id=$(do_start "WAF" "${zk}" "${waf_topic}")
                        answer "waf_id=${waf_id}"
                fi
                if [[ ${flow_err} -eq 0 ]];then
                        flow_id=$(do_start "FLOW" "${zk}" "${flow_topic}")
                        answer "flow_id=${flow_id}"
                fi
                if [[ ${scan_err} -eq 0 ]];then
                        scan_id=$(do_start "SCAN" "${zk}" "${scan_topic}")
                        answer "scan_id=${scan_id}"
                fi
                sleep 300
                do_kill ${ids_id} ${vds_id} ${waf_id} ${flow_id} ${scan_id}
                #do_kill ${vds_id}
                #do_kill ${waf_id}
                #do_kill ${flow_id}
                #do_kill ${scan_id}
        done
}
main "$@"

