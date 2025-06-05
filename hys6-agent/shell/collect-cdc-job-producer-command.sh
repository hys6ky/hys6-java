#!/bin/bash

#包发布的版本号
RELEASE_VERSION="6.1"
#通过该脚本调用集市作业（调度平台调用）
# shell script execution directory
SH_EXEC_DIR=$(
  cd "$(dirname "$0")" || exit
  pwd
)
# 采集包名
COLLECT_JAR_NAME="hyren-serv6-agent-collect-cdc.jar"
# 采集程序Main方法
MAIN_CLASS="hyren.serv6.agent.run.FLinkKafkaConsumerJobCommand"
# 采集程序kafka生产者Main方法
MAIN_PRODUCER_CLASS="hyren.serv6.agent.run.FlinkKafkaProducerJobCommand"
# 采集程序kafka消费者Main方法
MAIN_CONSUMER_CLASS="hyren.serv6.agent.run.FlinkKafkaConsumerJobCommand"
# Get system bits
OS_BIT=$(getconf LONG_BIT)
# 脚本标签
COLLECT_TAG="HYREN_CDC_COLLECT_JOB"
# 采集任务id
TASK_ID="${1}"
shift
# 采集表名
COLLECT_TABLE_NAMES="${1}"
shift
# 日志标识
LOG_IDENTIFICATION="${1}"
shift
LOG_OUT_FOLDER="${SH_EXEC_DIR}/../running/cdclog/${LOG_IDENTIFICATION}"
LOG_OUT_PRODUCER_FILE="${LOG_OUT_FOLDER}/producer_${TASK_ID}.log"



# 脚本运行入口
## 参数1：任务ID
## 参数2：表名
## 使用方式 sh collect-cdc-job-command.sh 899948182551662592 agent_info
## 返回状态码说明 {1: 程序包不存在}
main() {
  # if no parameter is passed to script then show how to use.
#  if [[ $# -eq 0 ]]; then usage; fi
  # Enter the script directory
  cd "${SH_EXEC_DIR}" || exit
  # Check the legality of the file
  if [[ ! -f ${COLLECT_JAR_NAME} ]]; then echo "Collect service package file does not exist, please check !" && exit 1; fi
  # execute script
  mkdir -p ${LOG_OUT_FOLDER}
  stop_collect
  collect_producer_main
}
stop_collect() {
  #!/bin/bash
  # 获取要杀死的所有进程的 ID
  pids=$(ps -ef | grep "${TASK_ID}" | grep "${COLLECT_TABLE_NAMES}" | grep "producer" | grep -v $$ | grep -v "grep" | awk '{print $2}')
  echo "ps -ef | grep ${TASK_ID} | grep ${COLLECT_TABLE_NAMES} | grep producer | grep -v $$ | grep -v 'grep'"
  # 判断 pids 是否为空
  if [[ -n "$pids" ]]; then
    # 杀死所有进程
    for pid in $pids; do
      echo "kill -9 $pid"
      kill -9 $pid
    done
  else
    echo "no producer program running."
  fi
}
collect_producer_main() {
  echo "*********collect_producer_main() - start***********"
  CLASSPATH=".:${SH_EXEC_DIR}/resources:${SH_EXEC_DIR}/${COLLECT_JAR_NAME}:"
  CLASSPATH="${CLASSPATH}${SH_EXEC_DIR}/collect-cdc-job-jars/*:"
  CLASSPATH="${CLASSPATH}${SH_EXEC_DIR}/../jdbc/*"
  nohup "${SH_EXEC_DIR}"/jre/linux/"${OS_BIT}"/jre/bin/java -Xms64m -Xmx1024m  \
    -Dproject.name="${COLLECT_TAG}" \
    -Dproject.id="${TASK_ID}" \
    -DbasePackage=agent \
    -DisMultiLineLog=true \
    -Djava.ext.dirs="${SH_EXEC_DIR}/../jdbc:${SH_EXEC_DIR}/jre/linux/${OS_BIT}/jre/lib/ext" \
    -Xbootclasspath/a:./resources \
    -cp "${CLASSPATH}" ${MAIN_PRODUCER_CLASS} \
    "${TASK_ID}" "${COLLECT_TABLE_NAMES}" producer \
    >>"${LOG_OUT_PRODUCER_FILE}" 2>&1 &
  echo "*********collect_producer_main() - end***********"
  # 验证是否启动成功
  verify_successful
}
# 验证是否启动成功
verify_successful() {
  
  # 定义超时时间（秒）
  p_timeout=30
  # 定义开始时间
  p_start_time=$(date +%s)
  
  # 监听 producer.log 文件
  while true
  do
    # 检查文件是否存在
    if [ -f ${LOG_OUT_PRODUCER_FILE} ]; then
      # 读取文件内容
      content=$(cat ${LOG_OUT_PRODUCER_FILE} )
      if [[ $content == *"[flink-cdc] producer-started"* ]];    # 正常启动 
      then
        echo [flink-cdc] producer-started
        exit 200
      elif [[ $content == *"[flink-cdc] producer-error"* ]];    # producer 启动失败
      then
        echo [flink-cdc] producer-error
        exit 210
      elif [[ $content == *"[flink-cdc] get-params-fail"* ]];    # producer 远程获取数据失败
      then
        echo [flink-cdc] get-params-fail
        exit 230
      elif [[ $content == *"[flink-cdc] checkpoint-url-iserror"* ]];    # producer 检查点地址错误
      then
        echo [flink-cdc] checkpoint-url-iserror
        exit 240
      elif [[ $content == *"[flink-cdc] main-params-error"* ]];    # producer main 方法参数异常
      then
        echo [flink-cdc] main-params-error
        exit 250
      fi
    fi
  
    # 获取当前时间
    p_current_time=$(date +%s)
    # 计算已经过去的时间
    p_elapsed_time=$((p_current_time - p_start_time))
    # 检查是否超过超时时间
    if [ $p_elapsed_time -ge $p_timeout ]; then
      echo "producer not running.please check."
      # 启动超时
      exit 220
    fi
    echo -n "."
  
    # 等待 1 秒
    sleep 1
  done
}

start_producer(){
  # 定义超时时间（秒）
  timeout=30
  # 定义开始时间
  start_time=$(date +%s)
  
  # 监听 consumer.log 文件
  while true
  do
    # 检查文件是否存在
    if [ -f ${LOG_OUT_CONSUMER_FILE} ]; then
      # 读取文件内容
      content=$(cat ${LOG_OUT_CONSUMER_FILE} )
      if [[ $content == *"[flink-cdc] consumer-started"* ]];    # 正常启动 
      then
        echo [flink-cdc-consumer] consumer-started
        # 执行函数 collect_producer_main
        collect_producer_main
        # 退出 while 循环
        
      elif [[ $content == *"[flink-cdc] consumer-no-jdbc"* ]];    # 没有 jdbc 信息，中止启动 
      then
        echo [flink-cdc-consumer] consumer-no-jdbc
        # 执行函数 collect_producer_main
        collect_producer_main
        # 退出 while 循环
        break
      elif [[ $content == *"[flink-cdc] main-params-error"* ]];   # main 方法异常 
      then
        echo [flink-cdc-consumer] main-params-error
        exit 120
      elif [[ $content == *"[flink-cdc] consumer-error"* ]];    # consumer 执行异常 
      then
        echo [flink-cdc-consumer] consumer-error
        exit 130
      elif [[ $content == *"[flink-cdc] jdbc-error"* ]];    # jdbc 连接异常 
      then
        echo [flink-cdc-consumer] jdbc-error
        exit 140
      fi
    fi
  
    # 获取当前时间
    current_time=$(date +%s)
    # 计算已经过去的时间
    elapsed_time=$((current_time - start_time))
    # 检查是否超过超时时间
    if [ $elapsed_time -ge $timeout ]; then
      echo "consumer not running.please check."
      # 启动超时
      exit 150
    fi
    echo -n "."
  
    # 等待 1 秒
    sleep 1
  done
}

#function usage means how to use this script.
usage() {
  echo "Usage: $0 TASK_ID COLLECT_TABLE_NAMES COLLECT_TYPE"
  echo 'for example: cdccollect-job-command.sh "899948182551662592" "agent_info"'
  exit
}

# 加载脚本
main "$@"
