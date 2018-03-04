#!/bin/bash

export PYTHON_EGG_CACHE=/tmp/.cache; export PYSPARK_PYTHON=/usr/bin/python2.7; spark2-submit --jars /home/apt/lib/spark-streaming-kafka-0-8-assembly_2.11-2.1.1.jar --master yarn --deploy-mode cluster --py-files pkg/docommand.py,pkg/doDebug.py,pkg/domysql.py,pkg/MySqlConn.py,pkg/threatCommand.py,conf/Config.py src/kafka_netflow.py

