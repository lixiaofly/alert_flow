#!/bin/sh

outFile() {
	local topic="$1"
	local part="$2"
	local broker="$3"
	#local result=0
	local outfile="/tmp/${topic}"

	result=$(kafka-run-class kafka.tools.GetOffsetShell \
	        --broker-list "${broker}" \
	        -topic "${topic}" \
	        --time -1)
	echo "${result}" >${outfile}
}
main(){
	local topic="$1"
	local part="$2"
	local broker="$3"
	local outfile="/tmp/${topic}"
	local i=0
	touch "${outfile}" 2>/dev/null
	chmod 777 "${outfile}"
	while [[ $i < 3 ]]; do
		if [[ "${part}" == "0" ]];then
			outFile "${topic}" "${part}" "${broker}"
		fi
		ret=$(cat "${outfile}" 2>/dev/null|awk -F "${topic}:${part}:" '{print $2}')
#		echo "ret=",$ret
		if [[ "${part}" != "0" && -z "${ret}" ]];then
			outFile "${topic}" "${part}" "${broker}"
		
		elif [[  ${ret} -ne 0 ]]; then
			break
		fi
#		echo "i=",$i
		((++i))
	done
    
	if [[ -z "${ret}" ]];then
		ret=0
	fi
    
	echo ${ret}
	return ${ret}
}

main "$@"
