#!/bin/bash
. /etc/profile
main() {
        #export PYSPARK_PYTHON=/usr/bin/python2.7
        local idsFile="kafka_ids.py"
        local vdsFile="kafka_vds.py"
        local wafFile="kafka_waf.py"
        local netflowFile="kafka_netflow.py"
        local err=0
        local id=""
        local path=${APT_PACKAGE}/spark_app/alert_flow
        local pkg_path=${path}/pkg
        local conf_path=${path}/conf
        local src_path=${path}/src

        local hosts=$(cat /etc/hosts |awk '{print $1}' |grep -v "127.0.0.1")
        ${path}/src/pre.sh "aaaaaa" "${src_path}/getOffset.sh"  ${hosts}; local err_pre=$?
        if [[ ${err_pre} -ne 0 ]];then
                return 1
        fi

        #do_write_conf; local err_conf=$?
        #if [[ ${err_conf} -ne 0 ]];then
                #return 1
        #fi
        
        for file in "domysql.py" "docommand.py" "MySqlConn.py"; do
                if [[ ! -f "${pkg_path}/${file}" ]]; then
                        echo "ERROR: ${pkg_path}/${file} is not exist!!!"
                        return 1
                fi
        done
        if [[ ! -f "${conf_path}/Config.py" ]]; then
                echo "ERROR: ${conf_path}/Config.py is not exist !!!"
                return 1
        fi
        for file in ${idsFile} ${vdsFile} ${wafFile} ${netflowFile}; do
        #for file in ${idsFile};do
                if [[ -f "${src_path}/${file}" ]]; then
                        yarn application -list |grep "${file}" >/dev/null; err=$?
                        
                        if [[ ${err} == 0 ]]; then
                                id=$(yarn application -list |grep "${file}" |awk '{print $1}')
                                #	echo ${id}
                                for a in ${id};do
                                        yarn application -kill ${a}
                                done
                        fi
                        sudo -u hdfs spark2-submit \
                        --jars ${APT_HOME}/lib/spark-streaming-kafka-0-8-assembly_2.11-2.1.1.jar \
                        --master yarn --deploy-mode cluster \
                        --py-files ${pkg_path}/docommand.py,${pkg_path}/domysql.py,${pkg_path}/MySqlConn.py,${pkg_path}/doDebug.py,${conf_path}/Config.py,\
                        --driver-memory 3g \
                        --executor-memory 3g \
                        --num-executors 5 \
                        --executor-cores 4 \
                        --conf spark.streaming.concurrentJobs=55 \
                        --conf spark.streaming.kafka.maxRatePerPartition=500 \
                        --conf  spark.yarn.executor.memoryOverhead=1024 \
                        --conf spark.yarn.driver.memoryOverhead=1024 \
                        --conf spark.speculation=true \
                        --conf spark.speculation.interval=100 \
                        --conf spark.speculation.quantile=0.75 \
                        --conf spark.speculation.multiplier=1.5 \
                        ${src_path}/${file} &
                        sleep 8
                fi
        done
}

main "$@"

