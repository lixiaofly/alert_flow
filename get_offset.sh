#!/bin/bash

NEWOFFSET=""

main()
{
        local topic="$1"; shift 1
        local part=0
        local name=""
        local offset=0
        local outinfo=""
        
        for info in $(kafka-run-class kafka.tools.GetOffsetShell \
                       --broker-list 192.168.1.106:9092 \
                       -topic ${topic} \
                       --time -1 2>/dev/null); do
                part=$(echo ${info} |awk -F ':' '{print $2}')
                name="${topic}${part}"
                offset=$(echo ${info} |awk -F ':' '{print $3}')
                outinfo="\"${name}\":${offset}"
                NEWOFFSET=${NEWOFFSET} + ${outinfo}
        done
        NEWINFO="{" + ${NEWOFFSET} + "}"
        
        echo "${NEWOFFSET}"
}

main "$@"
