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
COLLECT_JAR_NAME="hyren-serv6-agent-${RELEASE_VERSION}.jar"
# 采集程序Main方法
MAIN_CLASS="hyren.serv6.agent.run.SemiStructuredJobCommand"
# HADOOP_OPTS
HADOOP_OPTS="-Djava.library.path=/opt/cloudera/parcels/CDH/lib/hadoop/lib/native"
# Get system bits
OS_BIT=$(getconf LONG_BIT)
# 脚本标签
COLLECT_TAG="HYREN_SEMISTRUCTURED_JOB"
# 采集任务id
TASK_ID="${1}"
# 跑批日期
ETL_DATE="${2}"

# 脚本运行入口
## 参数1：任务ID
## 使用方式 sh semistructured-job-command.sh 899948182551662592 20230110
## 返回状态码说明 {1: 程序包不存在}
main() {
  # if no parameter is passed to script then show how to use.
  if [[ $# -eq 0 ]]; then usage; fi
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
  CLASSPATH=".:${SH_EXEC_DIR}/resources"
  CLASSPATH="${CLASSPATH}:${SH_EXEC_DIR}/${COLLECT_JAR_NAME}"
  echo "${CLASSPATH}"
  "${SH_EXEC_DIR}"/jre/linux/"${OS_BIT}"/jre/bin/java -Xms64m -Xmx1024m ${HADOOP_OPTS} \
    -Dproject.name="${COLLECT_TAG}" \
    -Dproject.id="${TASK_ID}" -DbasePackage=unstructured -DisMultiLineLog=true \
    -DbasePackage=agent \
    -DisMultiLineLog=true \
    -Djava.ext.dirs="${SH_EXEC_DIR}/../jdbc:${SH_EXEC_DIR}/../jars:${SH_EXEC_DIR}/jre/linux/${OS_BIT}/jre/lib/ext" \
    -Xbootclasspath/a:./resources \
    -cp "${CLASSPATH}" -Dloader.main="${MAIN_CLASS}" org.springframework.boot.loader.PropertiesLauncher \
    "${TASK_ID}" "${ETL_DATE}"
}

#function usage means how to use this script.
function usage() {
  echo "Usage: $0 ODC_ID ETL_DATE"
  echo "for example: $0 899948182551662592 20230110"
  exit
}

# 加载脚本
main "$@"
