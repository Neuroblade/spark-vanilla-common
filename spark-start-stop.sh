#!/bin/bash

set -e

here="$(realpath $(dirname $0))"

usage="
Start Spark Services HELP

usage: "$(basename $0)" [history]

OPTIONS

   -h,  -help                 show this message
   -stop		      if given, servers will be stopped
   -start		      if given, servers will be started
   -history	              if given, history server also will be executed/killed
"


# Get arguments
while [[ $# -gt 0 ]]; do
	case $1 in
		-h |-help) echo "$usage"; exit 0 ;;
		-history) _history_server=true; shift;;
		-start) _start=true; shift;;
		-stop) _stop=true; shift;;
		$'\n') shift ; break ;;
		*) echo "$usage"; exit 0;;
	esac

done


if ! [ -z $_start ]
then
	host=`echo $HOSTNAME`
	hostname="$host.neuroblade.corp"
	
	echo "------------------------------------------------"
	echo "------------------------------------------------"
	echo -e "\n\nRunning Spark Master..... \n\n"
	res=`jps | grep Master | cut -d" " -f2`
	if [[ $res == *"Master"* ]]
       	then 
		echo -e "Master is already running\n\n"
	else
		start-master.sh
		echo -e "\nYou can see it in its web UI on http://nblab48.neuroblade.corp:8080\nAlso, you can veirfy it's running by using jps command\n"
	fi

	echo "------------------------------------------------"
	echo "------------------------------------------------"
	echo -e "\n\n\nRunning Spark Worker..... \n\n"

	res=`jps | grep Worker | cut -d" " -f2`
	if [[ $res == *"Worker"* ]]
       	then 
		echo -e "Worker is already running\n\n"
	else
		start-worker.sh spark://${hostname}:7077
		echo -e "You can see in http://nblab48.neuroblade.corp:8080 that you have one worker connected and ALIVE!\n\n"
	fi

	if ! [ -z $_history_server ] 
	then
		echo "------------------------------------------------"
		echo "------------------------------------------------"
		echo -e "\nRunning Spark History Server (SHS)... \nIt will write logs to /dataset/spark-event-logs\n\n"

		res=`jps | grep History | cut -d" " -f2`
		if [[ $res == *"History"* ]]
	       	then 
			echo -e "History Server is already running\n\n"
		else
			start-history-server.sh
			echo -e "You can see in http://nblab48.neuroblade.corp:18080 the history server!\n\n"
			echo -e "\n\n"
		fi
	fi
elif ! [ -z $_stop ]
then

	echo "------------------------------------------------"
	echo "------------------------------------------------"
        echo -e "\n\nStopping Spark Worker..... \n\n"

        stop-worker.sh

	echo "------------------------------------------------"
	echo "------------------------------------------------"
        echo -e "\n\n\nStopping Spark Master..... \n\n"
        stop-master.sh
	echo -e "\n\n"

        if ! [ -z $_history_server ]
        then
		
		echo "------------------------------------------------"
		echo "------------------------------------------------"
		echo -e "\nStopping Spark History Server (SHS)...\n\n"
                stop-history-server.sh
                echo -e "\n\n"
        fi
	echo -e "Verify wanted services are down by running jps command\n"
else	
	echo -e "Commnad is unknown, please give -start or -stop flag\n\n"
fi
