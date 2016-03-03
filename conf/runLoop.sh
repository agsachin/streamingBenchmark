#!/usr/bin/env bash

flag=true

#cluster
runBenchmarkScript="/opt/install/streamingBenchmark/conf/runBenchmark.sh"

#local
#runBenchmarkScript="/Users/sachin/Documents/github/streamingBenchmark/conf/runBenchmark.sh"

while(${flag})
do

echo "**********check if any spark app is running**********"
sparkAppCount=$((`ps aux | grep TwitterStreaming | grep -v grep  |wc -l`))
if [ "$?" = "0" ]; then
	echo "Sucess!!"
else
	echo "failed!!"
	exit -1
fi

if [ "${sparkAppCount}" == "0" ]
  then
    echo "no Spark App is running !!";
  else
    echo "there are already ${sparkAppCount} spark app are running!!";
    flag=false
fi


echo "**********check if any push to kafka is running**********"

on75=1
on75=$((`pssh -H 169.45.101.75  -i "ps aux | grep PushToKafka | grep -v grep | wc -l "|grep -v SUCCESS|grep -v Stderr`))
if [ "$?" = "0" ]; then
	echo "Sucess!!"
else
	echo "failed!!"
	exit -1
fi

if [[ "${on75}" == "0" ]]; then
   echo "no PushToKafka on 75!!"
else
	echo "failed on 75!!"
	flag=false
fi

on67=1
on67=$((`pssh -H 169.45.101.67  -i "ps aux | grep PushToKafka | grep -v grep | wc -l "|grep -v SUCCESS|grep -v Stderr`))
if [ "$?" = "0" ]; then
	echo "Sucess!!"
else
	echo "failed!!"
	exit -1
fi

if [[ "${on67}" == "0" ]]; then
  echo "no PushToKafka on 67!!"
else
	echo "failed on 67 !!"
	flag=false
fi

on73=1
on73=$((`pssh -H 169.45.101.73  -i "ps aux | grep PushToKafka | grep -v grep | wc -l "|grep -v SUCCESS|grep -v Stderr`))
if [ "$?" = "0" ]; then
	echo "Sucess!!"
else
	echo "failed!!"
	exit -1
fi

if [[ "${on73}" == "0" ]]; then
  echo "no PushToKafka on 73!!"
else
	echo "failed on 73!!"
	flag=false
fi



if [ ${flag} == "false" ]; then
  echo "sleeping for 120 second `date`"
  sleep 120s
  flag=true
else
  echo "execute Job"
  sh ${runBenchmarkScript} --runSparkSubmit 1 2000 30 3333334
  sleep 30s
fi

done