#!/bin/bash

source /etc/profile

IDSINTERVAL=5
VDSINTERVAL=5
WAFINTERVAL=5
FLOWINTERVAL=5
SCANINTERVAL=5

ALLCOUNT=300
IDSPART=0
VDSPART=0
WAFPART=0
FLOWPART=0
SCANPART=0

echo_console()
{
        echo "$@" >/dev/tty
}

get_local_part_count()
{
        local file="$1"
        local count=0

        case ${file} in
        "kafka_netflow.py")
                count=${FLOWPART}
                ;;
        "kafka_ids.py")
                count=${IDSPART}
                ;;
        "kafka_vds.py")
                count=${VDSPART}
                ;;
        "kafka_waf.py")
                count=${WAFPART}
                ;;
        "portscan.py")
                count=${SCANPART}
                ;;
        esac

        echo ${count}
}

get_cur_jobs()
{
        local exec=$1
        local core=$2
        local num3=$((exec*core*3))
        local jobs=0

        if [[ ${num3} -ge 20 ]];then
                jobs=$((num3-2))
        elif [[ ${num3} -ge 10 ]];then
                jobs=$((num3-1))
        else
                jobs=${num3}
        fi

        echo ${jobs}
}

get_conf_name()
{
        local file="$1"
        local conf_name=""

        case ${file} in
        "kafka_netflow.py")
                conf_name="FLOWPARTITION"
                ;;
        "kafka_ids.py")
                conf_name="IDSPARTITION"
                ;;
        "kafka_vds.py")
                conf_name="VDSPARTITION"
                ;;
        "kafka_waf.py")
                conf_name="WAFPARTITION"
                ;;
        "portscan.py")
                conf_name="SCANPARTITION"
                ;;
        esac

        echo "${conf_name}"
}

get_partion_count()
{
        local file="$1"
        local conf_path="$2"
        local count=1
        local local_count=$(get_local_part_count "${file}")

        if [[ ${local_count} -eq 0 ]];then
                local name=$(get_conf_name "${file}")

                if [[ ! -z "${name}" ]];then
                        count=$(cat ${conf_path}/Config.py |grep -v "^#" |grep -w "${name}" |awk -F '=' '{print $2}')
                fi
                if [[ -z "${count}" ]];then
                        count = 1
                fi
        else
                count=${local_count}
        fi

        echo ${count}
}

get_topic_count()
{
        local file="$1"
        local count=1

        case ${file} in
        "kafka_netflow.py")
                count=3
                ;;
        "kafka_ids.py" | "kafka_vds.py" | "kafka_waf.py" | "portscan.py")
                count=1
                ;;
        *)
                count=1
                ;;
        esac

        echo ${count}
}

get_rate()
{
        local file="$1"
        local path="$2"
        local rate=100

        local tcount=$(get_topic_count "${file}")
        local pcount=$(get_partion_count "${file}" "${path}")

        #echo_console "tcount=${tcount}"
        #echo_console "pcount=${pcount}"
        #echo_console "ALLCOUNT=${ALLCOUNT}"

        local rate=$((ALLCOUNT/tcount/pcount))

        echo ${rate}
}

main() {
        export PYSPARK_PYTHON=/usr/bin/python2.7
        export PYTHON_EGG_CACHE=/tmp/.cache
        local drv_memory="5g"
        local executor_memory="3g"
        local num_executor=2
        local executor_core=4
        #local cur_jobs=$(get_cur_jobs ${num_executor} ${executor_core})
        local cur_jobs=1
        local alert_rate=100
        local flow_rate=800
        local portscan_rate=1000

        local flow_memory="5g"
        local flow_executor=3
        local flow_jobs=4

        local type="$1"
        local path="$2"
        local file="$3"; shift 3
        local err=0
        local id=""
        local pkg_path="${path}/pkg"
        local conf_path="${path}/conf"
        local src_path="${path}/src"

        #local rate=$(get_rate "${file}" "${conf_path}")

        #echo "num_executor=${num_executor}"
        #echo "executor_core=${executor_core}"
        #echo "rate=${rate}"
        #echo "jobs=${cur_jobs}"

        if [[ "${type}" == "auto" ]]; then
                local jar_path="${APT_HOME}/lib"
        else
                local jar_path="${path}/package"
        fi

        local kafka_jar="${jar_path}/spark-streaming-kafka-0-8-assembly_2.11-2.1.1.jar"
        if [[ ! -f "${kafka_jar}" ]];then
                echo "${kafka_jar} is not exist."
                return 1
        fi

        if [[ -f "${src_path}/${file}" ]]; then
                case ${file} in
                "portscan.py")
                spark-submit \
                --jars ${kafka_jar} \
                --master yarn --deploy-mode cluster \
                --py-files ${pkg_path}/docommand.py,${pkg_path}/domysql.py,${pkg_path}/MySqlConn.py,${pkg_path}/doDebug.py,${pkg_path}/threatCommand.py,${conf_path}/Config.py,\
                --conf spark.driver.extraJavaOptions=" -XX:MaxPermSize=1G " \
                --conf spark.executor.extraJavaOptions=" -XX:MaxPermSize=2048m -XX:PermSize=256m" \
                --driver-memory ${drv_memory} \
                --executor-memory ${flow_memory} \
                --num-executors ${flow_executor} \
                --executor-cores ${executor_core} \
                --conf spark.streaming.concurrentJobs=${flow_jobs} \
                --conf spark.streaming.receiver.maxRate=${portscan_rate} \
                ${src_path}/${file}
                sleep 8
                ;;
                "kafka_netflow.py")
                spark-submit \
                --jars ${kafka_jar} \
                --master yarn --deploy-mode cluster \
                --py-files ${pkg_path}/docommand.py,${pkg_path}/domysql.py,${pkg_path}/MySqlConn.py,${pkg_path}/doDebug.py,${pkg_path}/threatCommand.py,${conf_path}/Config.py,\
                --conf spark.driver.extraJavaOptions=" -XX:MaxPermSize=1G " \
                --conf spark.executor.extraJavaOptions=" -XX:MaxPermSize=2048m -XX:PermSize=256m" \
                --driver-memory ${drv_memory} \
                --executor-memory ${flow_memory} \
                --num-executors ${flow_executor} \
                --executor-cores ${executor_core} \
                --conf spark.streaming.concurrentJobs=${flow_jobs} \
                --conf spark.streaming.receiver.maxRate=${flow_rate} \
                ${src_path}/${file}
                ;;
                *)
                spark-submit \
                --jars ${kafka_jar} \
                --master yarn --deploy-mode cluster \
                --py-files ${pkg_path}/docommand.py,${pkg_path}/domysql.py,${pkg_path}/MySqlConn.py,${pkg_path}/doDebug.py,${pkg_path}/threatCommand.py,${conf_path}/Config.py,\
                --conf spark.driver.extraJavaOptions=" -XX:MaxPermSize=1G " \
                --conf spark.executor.extraJavaOptions=" -XX:MaxPermSize=2048m -XX:PermSize=256m" \
                --driver-memory ${drv_memory} \
                --executor-memory ${executor_memory} \
                --num-executors ${num_executor} \
                --executor-cores ${executor_core} \
                --conf spark.streaming.concurrentJobs=${cur_jobs} \
                --conf spark.streaming.kafka.maxRatePerPartition=${alert_rate} \
                ${src_path}/${file}
                ;;
                esac
        fi
}

main "$@"

