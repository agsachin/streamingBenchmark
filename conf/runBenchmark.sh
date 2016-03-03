#!/usr/bin/env bash


echo "**********move to home directory**********"
cd ;
if [ "$?" = "0" ]; then
	echo "Sucess!!"
else
	echo "failed!!"
	exit -1
fi


#cluster
#sparkMaster="spark://spark-25.softlayer.com:7077"
#confFile="/opt/install/streamingBenchmark/conf/benchmarkConf.yaml"
#sparkSubmit="/opt/install/spark-1.6.0-bin-hadoop2.6/bin/spark-submit"
#sparkBenchmarkJar="/opt/install/streamingBenchmark/spark-benchmarks/target/spark-benchmarks-0.1.0.jar"
#kafkaHostFile="/opt/install/streamingBenchmark/conf/kafka-host.txt"
#sparkHostFile="/opt/install/streamingBenchmark/conf/spark-host.txt"
#pushToKafkaJar="/opt/install/streamingBenchmark/push-to-kafka/target/push-to-kafka-0.1.0.jar"
#zookeeperStartScript="/opt/install/zookeeper-3.4.6/bin/zkServer.sh"
#kafkaStartScript="/opt/install/kafka_2.10-0.8.2.2/bin/kafka-server-start.sh"
#kafkaProperties="/opt/install/kafka_2.10-0.8.2.2/config/server.properties"
#kafkaStartLog=" /tmp/kafka/kafka-start.log"


#local
sparkMaster=" spark://sachins-MacBook-Pro.local:7077"
confFile="/Users/sachin/Documents/github/streamingBenchmark/conf/benchmarkConfLocal.yaml"
sparkSubmit="/Users/sachin/spark_local/spark-1.5.0-bin-hadoop2.6/bin/spark-submit"
sparkBenchmarkJar="/Users/sachin/Documents/github/streamingBenchmark/spark-benchmarks/target/spark-benchmarks-0.1.0.jar"
kafkaHostFile="/Users/sachin/Documents/github/streamingBenchmark/conf/kafka-host.txt"
sparkHostFile="/Users/sachin/Documents/github/streamingBenchmark/conf/spark-host.txt"
pushToKafkaJar="/Users/sachin/Documents/github/streamingBenchmark/push-to-kafka/target/push-to-kafka-0.1.0.jar"
zookeeperStartScript=""
kafkaStartScript=""
kafkaProperties=""
kafkaStartLog=""


runSparkSubmit(){
echo "**********print Params received**********"
processId=$1
performanceBatchTime=$2
kafkaLoaderThread=$3
kafkaLoaderThreadLimit=$4

echo ${processId}
echo ${performanceBatchTime}
echo ${kafkaLoaderThread}
echo ${kafkaLoaderThreadLimit}


echo "**********copy backup file to conf file**********"
cp ${confFile}.bkp ${confFile}
if [ "$?" = "0" ]; then
	echo "Success!!"
else
	echo "failed!!"
	exit -1
fi


echo "**********creating conf file**********"
echo "sed -i -e 's/#performanceBatchTime#/'${performanceBatchTime}'/g;s/#kafkaLoaderThread#/'${kafkaLoaderThread}'/g;s/#kafkaLoaderThreadLimit#/'${kafkaLoaderThreadLimit}'/g' ${confFile}"
`sed -i -e 's/#performanceBatchTime#/'${performanceBatchTime}'/g;s/#kafkaLoaderThread#/'${kafkaLoaderThread}'/g;s/#kafkaLoaderThreadLimit#/'${kafkaLoaderThreadLimit}'/g' ${confFile}`
if [ "$?" = "0" ]; then
	echo "Success!!"
else
	echo "failed!!"
	exit -1
fi

echo "**********validating conf file**********"
echo "cat ${confFile} |grep spark.performance.batchTime"
bachTimeLine=$(cat ${confFile} |grep 'spark.performance.batchTime' )
if [[ ${bachTimeLine} == *${performanceBatchTime}* ]]
  then
    echo "validation passed!!";
  else
    echo "validation failed!!";
    exit -1
fi

echo "**********launch spark submit**********"
echo "/usr/bin/nohup ${sparkSubmit} --class spark.benchmark.TwitterStreaming --master ${sparkMaster} ${sparkBenchmarkJar} ${confFile} 2>&1 &"
`/usr/bin/nohup ${sparkSubmit} --class spark.benchmark.TwitterStreaming --master ${sparkMaster} ${sparkBenchmarkJar} ${confFile} \> sparkSubmit_${processId}.log 2\>&1 &`

echo "**********validating spark submit**********"
echo "cat sparkSubmit_${processId}.log |tail -100|grep Exception"
tailString=`cat sparkSubmit_${processId}.log |tail -100`
if [[ ${tailString} == *"Exception"* ]]; then
  echo "failed!!"
	exit -1
else
	echo "Success!!"
fi

}


runPushToKafka(){
echo "**********print Params received**********"
processId=$1
performanceBatchTime=$2
kafkaLoaderThread=$3
kafkaLoaderThreadLimit=$4

if (( $kafkaLoaderThread % 3 == 0 ))  ; then
	kafkaLoaderThread=$((kafkaLoaderThread/3))
	else
	echo "kafkaLoaderThread must be multiple of 3 to start equal threads on each machine"
	echo "${kafkaLoaderThread} is not multiple of 3"
	exit -2
fi

echo ${processId}
echo ${performanceBatchTime}
echo ${kafkaLoaderThread}
echo ${kafkaLoaderThreadLimit}

echo "**********copy backup file to conf file**********"
cp ${confFile}.bkp ${confFile}
if [ "$?" = "0" ]; then
	echo "Success!!"
else
	echo "failed!!"
	exit -1
fi

echo "**********creating conf file**********"
echo "sed -i -e 's/#performanceBatchTime#/'${performanceBatchTime}'/g;s/#kafkaLoaderThread#/'${kafkaLoaderThread}'/g;s/#kafkaLoaderThreadLimit#/'${kafkaLoaderThreadLimit}'/g' ${confFile}"
`sed -i -e 's/#performanceBatchTime#/'${performanceBatchTime}'/g;s/#kafkaLoaderThread#/'${kafkaLoaderThread}'/g;s/#kafkaLoaderThreadLimit#/'${kafkaLoaderThreadLimit}'/g' ${confFile}`
if [ "$?" = "0" ]; then
	echo "Success!!"
else
	echo "failed!!"
	exit -1
fi

echo "**********validating conf file**********"
echo "cat ${confFile} |grep spark.performance.batchTime"
bachTimeLine=$(cat ${confFile} |grep 'spark.performance.batchTime' )
if [[ ${bachTimeLine} == *${performanceBatchTime}* ]]
  then
    echo "validation passed!!";
  else
    echo "validation failed!!";
    exit -1
fi


echo "**********push conf file to all host**********"
pssh -h ${kafkaHostFile} -i "scp 10.168.102.160:${confFile} ${confFile}"
if [ "$?" = "0" ]; then
	echo "Success!!"
else
	echo "failed!!"
	exit -1
fi


echo "**********launch push to kafka operation**********"
`pssh -h ${kafkaHostFile} -i "nohup java -cp ${pushToKafkaJar}  benchmark.common.kafkaPush.PushToKafka ${confFile} > pushToKafka_${processId}.log 2>&1 &"`
if [ "$?" = "0" ]; then
	echo "Success!!"
else
	echo "failed!!"
	exit -1
fi


echo "**********validating push to kafka operation**********"
tailString=`pssh -h ${kafkaHostFile} -i "cat pushToKafka_${processId}.log |tail -100"`
if [[ ${tailString} == *"Exception"* ]]; then
  echo "failed!!"
	exit -1
else
	echo "Success!!"
fi

}

killSparkBenchmarkJob() {
echo "**********kill spark benchmark job**********"
ps aux | grep TwitterStreaming  |grep -v grep | head -1 | cut -d ' ' -f 2 | xargs kill -9
if [ "$?" = "0" ]; then
	echo "Success1!!"
else
  ps aux | grep TwitterStreaming  |grep -v grep | head -1 | cut -d ' ' -f 3 | xargs kill -9
  if [ "$?" = "0" ]; then
    echo "Success2!!"
  else
    ps aux | grep TwitterStreaming  |grep -v grep | head -1 | cut -d ' ' -f 3 | xargs kill -9
    if [ "$?" = "0" ]; then
      echo "Success3!!"
    else
      echo "failed!!"
      exit -1
    fi
  fi
fi

}


cleanKafka(){
echo "**********delete data and kill kafka step1**********"
pssh -h ${kafkaHostFile} -i "rm -r  /opt/install/kafka_2.10-0.8.2.2/logs/*"
if [ "$?" = "0" ]; then
	echo "Success!!"
else
	echo "failed!!"
	exit -1
fi
echo "**********delete data and kill kafka step2**********"
pssh -h ${kafkaHostFile} -i "rm -r  /tmp/kafka-logs-*/"
if [ "$?" = "0" ]; then
	echo "Success!!"
else
	echo "failed!!"
	exit -1
fi


echo "**********validate kafka is killed**********"
countOfProcess=`pssh -h ${kafkaHostFile} -i "ps aux | grep zoo.cfg|grep -v grep" |grep -v SUCCESS |grep -v grep|grep -v Stderr |wc -l`
if [ "$?" = "0" ]; then
	echo "Success!!"
else
	echo "failed!!"
	exit -1
fi

if [ ${countOfProcess} > 0 ]; then
	echo "validation failed!!"
	exit -1
else
	echo "validation passed!!"
fi

}

startZookeper(){
echo "**********starting zookeeper**********"
pssh -h ${kafkaHostFile} -i "${zookeeperStartScript} start"
if [ "$?" = "0" ]; then
	echo "Success!!"
else
	echo "failed!!"
	exit -1
fi

echo "**********validate process is running**********"
countOfProcess=`pssh -h ${kafkaHostFile} -i "ps aux | grep zoo.cfg|grep -v grep" |grep -v SUCCESS |grep -v grep|grep -v Stderr |wc -l`
if [ "$?" = "0" ]; then
	echo "Success!!"
else
	echo "failed!!"
	exit -1
fi

if [ "${countOfProcess}" = "3" ]; then
	echo "validation passed!!"
else
	echo "validation failed!!"
	exit -1
fi

}


startKafka(){
echo "**********starting kafka**********"
pssh -h host.txt -i " nohup ${kafkaStartScript} ${kafkaProperties} > ${kafkaStartLog} 2>&1 & "
if [ "$?" = "0" ]; then
	echo "Success!!"
else
	echo "failed!!"
	exit -1
fi


echo "**********validate process is running**********"
countOfProcess=`pssh -h ${kafkaHostFile} -i "ps aux | grep kafka|grep server.properties|grep -v grep" |grep -v SUCCESS |grep -v grep|grep -v Stderr |wc -l`
if [ "$?" = "0" ]; then
	echo "Success!!"
else
	echo "failed!!"
	exit -1
fi

if [ "${countOfProcess}" = "3" ]; then
	echo "validation passed!!"
else
	echo "validation failed!!"
	exit -1
fi

}
#killSparkBenchmarkJob
#runSparkSubmit 1 2000 30 3333334
#runPushToKafka 1 2000 30 3333334
#mv /Users/sachin/Documents/github/streamingBenchmark/conf/benchmarkConf.yaml-e /Users/sachin/Documents/github/streamingBenchmark/conf/benchmarkConf.yaml