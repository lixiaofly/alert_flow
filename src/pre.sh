#!/bin/bash
usage() {
	echo "./pre [PASSWD] [FILE] [IPLIST]"
}
main() {
	local passwd="$1";
	local file="$2"; shift 2
	local argv="$*"
	local count="$#"
#	echo $passwd,$argv,$count

	if [[ $count -ge 1 ]];then
		#cp -fRp ${APT_PACKAGE}/alert_flow/src/getOffset.sh /home/
		cp -fRp "${file}" /home/
		for ip in $argv;
		do
			echo "OK"
			#sshpass -p ${passwd} scp ${APT_PACKAGE}/alert_flow/src/getOffset.sh root@"${ip}":/home/
			sshpass -p ${passwd} scp -o StrictHostKeyChecking=no -o ConnectTimeout=60 ${file} root@"${ip}":/home/
		done
	else
		usage
	fi
}
main "$@"
