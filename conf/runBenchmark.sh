#!/bin/bash

echo "**********move to home directory**********"
cd ;
if [ "$?" = "0" ]; then
	echo "Sucess!!"
else
	echo "failed!!"
	exit -1
fi


#cluster
sparkMaster="spark://spark-25.softlayer.com:7077"
confFile="/opt/install/streamingBenchmark/conf/benchmarkConf.yaml"
sparkSubmit="/opt/install/spark-1.6.0-bin-hadoop2.6/bin/spark-submit"
sparkBenchmarkJar="/opt/install/streamingBenchmark/spark-benchmarks/target/spark-benchmarks-0.1.0.jar"
kafkaHostFile="/opt/install/streamingBenchmark/conf/kafka-host.txt"
sparkHostFile="/opt/install/streamingBenchmark/conf/spark-host.txt"
pushToKafkaJar="/opt/install/streamingBenchmark/push-to-kafka/target/push-to-kafka-0.1.0.jar"
zookeeperStartScript="/opt/install/zookeeper-3.4.6/bin/zkServer.sh"
kafkaStartScript="/opt/install/kafka_2.10-0.8.2.2/bin/kafka-server-start.sh"
kafkaProperties="/opt/install/kafka_2.10-0.8.2.2/config/server.properties"
kafkaStartLog=" /tmp/kafka/kafka-start.log"


#local
#sparkMaster=" spark://sachins-MacBook-Pro.local:7077"
#confFile="/Users/sachin/Documents/github/streamingBenchmark/conf/benchmarkConfLocal.yaml"
#sparkSubmit="/Users/sachin/spark_local/spark-1.5.0-bin-hadoop2.6/bin/spark-submit"
#sparkBenchmarkJar="/Users/sachin/Documents/github/streamingBenchmark/spark-benchmarks/target/spark-benchmarks-0.1.0.jar"
#kafkaHostFile="/Users/sachin/Documents/github/streamingBenchmark/conf/kafka-host.txt"
#sparkHostFile="/Users/sachin/Documents/github/streamingBenchmark/conf/spark-host.txt"
#pushToKafkaJar="/Users/sachin/Documents/github/streamingBenchmark/push-to-kafka/target/push-to-kafka-0.1.0.jar"
#zookeeperStartScript=""
#kafkaStartScript=""
#kafkaProperties=""
#kafkaStartLog=""


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
echo "nohup ${sparkSubmit} --class spark.benchmark.TwitterStreaming --master ${sparkMaster} ${sparkBenchmarkJar} ${confFile} 2>&1 &"
`nohup ${sparkSubmit} --class spark.benchmark.TwitterStreaming --master ${sparkMaster} ${sparkBenchmarkJar} ${confFile} > sparkSubmit_${processId}.log 2>&1 &`

echo "**********validating spark submit**********"
echo "cat sparkSubmit_${processId}.log |tail -100|grep Exception"
tailString=`cat ~/sparkSubmit_${processId}.log |tail -100`
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
pssh -h ${kafkaHostFile} -i "scp 169.45.101.75:${confFile} ${confFile}"
if [ "$?" = "0" ]; then
	echo "Success!!"
else
	echo "failed!!"
	exit -1
fi


echo "**********launch push to kafka operation**********"
echo "pssh -h ${kafkaHostFile} -i nohup java -cp ${pushToKafkaJar}  benchmark.common.kafkaPush.PushToKafka ${confFile} > pushToKafka_${processId}.log 2>&1 &"
pssh -h ${kafkaHostFile} -i "nohup java -cp ${pushToKafkaJar}  benchmark.common.kafkaPush.PushToKafka ${confFile} > pushToKafka_${processId}.log 2>&1 &"
if [ "$?" = "0" ]; then
	echo "Success!!"
else
	echo "failed!!"
	exit -1
fi


echo "**********validating push to kafka operation**********"
tailString=`pssh -h ${kafkaHostFile} -i "cat ~/pushToKafka_${processId}.log |tail -100"`
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
fi
echo "**********delete data and kill kafka step2**********"
pssh -h ${kafkaHostFile} -i "rm -r  /tmp/kafka-logs"
if [ "$?" = "0" ]; then
	echo "Success!!"
else
	echo "failed!!"
fi


echo "**********validate kafka is killed**********"
countOfProcess=`pssh -h ${kafkaHostFile} -i "ps aux | grep kafka|grep server.properties|grep -v grep|wc -l" |grep -v SUCCESS |grep -v grep|grep -v Stderr |wc -l`
if [ "$?" = "0" ]; then
	echo "Success!!"
else
	echo "failed!!"
	exit -1
fi

if [ "${countOfProcess}" == "0 0 0" ]; then
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
zookeeperCountOfProcess=`pssh -h ${kafkaHostFile} -i "ps aux | grep zoo.cfg|grep -v grep" |grep -v SUCCESS |grep -v grep|grep -v Stderr |wc -l`
if [ "$?" = "0" ]; then
	echo "Success!!"
else
	echo "failed!!"
	exit -1
fi

if [ "${zookeeperCountOfProcess}" = "3" ]; then
	echo "validation passed!!"
else
	echo "validation failed!!"
	exit -1
fi

}


startKafka(){
echo "**********starting kafka**********"
pssh -h ${kafkaHostFile} -i " nohup ${kafkaStartScript} ${kafkaProperties} > ${kafkaStartLog} 2>&1 & "
if [ "$?" = "0" ]; then
	echo "Success!!"
else
	echo "failed!!"
	exit -1
fi


echo "**********validate process is running**********"
kafkaCountOfProcess=`pssh -h ${kafkaHostFile} -i "ps aux | grep kafka|grep server.properties|grep -v grep" |grep -v SUCCESS |grep -v grep|grep -v Stderr |wc -l`
if [ "$?" = "0" ]; then
	echo "Success!!"
else
	echo "failed!!"
	exit -1
fi

if [ "${kafkaCountOfProcess}" = "3" ]; then
	echo "validation passed!!"
else
	echo "validation failed!!"
	exit -1
fi

}

   case $1 in
        "--killSparkBenchmarkJob" )
          killSparkBenchmarkJob;;
        "--runSparkSubmit" )
        if [ $# -eq 5 ]; then
             runSparkSubmit $2 $3 $4 $5
        else
            echo "invalid argument please pass processId,performanceBatchTime,kafkaLoaderThread,kafkaLoaderThreadLimit"
        fi;;
        "--runPushToKafka" )
        if [ $# -eq 5 ]; then
             runPushToKafka $2 $3 $4 $5
        else
            echo "invalid argument please pass processId,performanceBatchTime,kafkaLoaderThread,kafkaLoaderThreadLimit"
        fi;;
        "--startKafka" )
          startKafka;;
        "--startZookeper" )
          startZookeper;;
        "--cleanKafka" )
          cleanKafka;;
        "--restartKafkaCluster" )
          cleanKafka;startZookeper;startKafka;;
          *)
            echo $"Usage: $0 {
            --restartKafkaCluster (all steps: cleanKafka,startZookeper,startKafka)
            --killSparkBenchmarkJob
            --runSparkSubmit processId performanceBatchTime kafkaLoaderThread kafkaLoaderThreadLimit
            --runPushToKafka processId performanceBatchTime kafkaLoaderThread kafkaLoaderThreadLimit
            --startKafka
            --startZookeper
            --cleanKafka
            TODO add clean spark and add kill push to kafka
            }"
            exit 1
    esac

