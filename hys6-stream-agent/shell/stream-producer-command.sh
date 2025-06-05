#!/bin/bash

#包发布的版本号
RELEASE_VERSION="6.1"
#通过该脚本调用作业（调度平台调用）
# shell script execution directory
SH_EXEC_DIR=$(
  cd "$(dirname "$0")" || exit
  pwd
)
# 采集包名
COLLECT_JAR_NAME="hyren-serv6-stream-agent-${RELEASE_VERSION}.jar"
# 采集程序Main方法
MAIN_CLASS="hyren.serv6.stream.agent.producer.run.StreamProducerJobCommand"
# HADOOP_OPTS
HADOOP_OPTS="-Djava.library.path=/opt/cloudera/parcels/CDH/lib/hadoop/lib/native"
# Get system bits
OS_BIT=$(getconf LONG_BIT)
# 脚本标签
COLLECT_TAG="HYREN_STREAM_PRODUCER_JOB"
# 采集任务id
TASK_ID="${1}"
shift
#截取并处理参数中的SQL占位符
for ((i = 0; i < $#; i++)); do
  if [ "${1}" != "-P" ]; then
    SQL_PARAMS="$SQL_PARAMS ${1}"
    shift
  fi
done

# 脚本运行入口
## 参数1：任务ID
## 参数6-N：sql占位符参数 String[]{condition=value}
## 使用方式 sh stream-producer-command.sh 899948182551662592
## 返回状态码说明 {1: 程序包不存在}
main() {
  # if no parameter is passed to script then show how to use.
  if [[ ! -n "${TASK_ID}" ]]; then usage; fi
  # Enter the script directory
  cd "${SH_EXEC_DIR}" || exit
  # Check the legality of the file
  if [[ ! -f ${COLLECT_JAR_NAME} ]]; then echo "Collect service package file does not exist, please check !" && exit 1; fi
  # execute script
  collect_main
}

# 通过该脚本调用采集作业（调度平台调用）
function collect_main() {
  # Configure classpath
  CLASSPATH=".:${SH_EXEC_DIR}/resources:${SH_EXEC_DIR}/${COLLECT_JAR_NAME}:"
  CLASSPATH="${CLASSPATH}${SH_EXEC_DIR}/../lib/*.jar"
  CLASSPATH="${CLASSPATH}${SH_EXEC_DIR}/../jdbc/*"
  "${SH_EXEC_DIR}"/jre/linux/"${OS_BIT}"/jre/bin/java -Xms64m -Xmx1024m ${HADOOP_OPTS} \
    -Dproject.name="${COLLECT_TAG}" \
    -Dproject.id="${TASK_ID}" \
    -Djava.ext.dirs="${SH_EXEC_DIR}"/../jdbc:"${SH_EXEC_DIR}"/jre/linux/"${OS_BIT}"/jre/lib/ext \
    -Xbootclasspath/a:./resources \
    -cp "${CLASSPATH}" -Dloader.main="${MAIN_CLASS}" org.springframework.boot.loader.PropertiesLauncher \
    "${TASK_ID}"
}

#function usage means how to use this script.
function usage() {
  echo "Usage: $0 TASK_ID "
  echo 'for example: stream-producer-command.sh "899948182551662592"'
  exit
}

# 加载脚本
main "$@"
