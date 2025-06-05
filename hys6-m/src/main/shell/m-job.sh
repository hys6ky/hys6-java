#!/bin/bash

#包发布的版本号
RELEASE_VERSION="6.1"
#通过该脚本调用写DDL作业（调度平台调用）
# shell script execution directory
SH_EXEC_DIR=$(
  cd "$(dirname "$0")" || exit
  pwd
)
#SH_EXEC_DIR=$(which java)
echo ${SH_EXEC_DIR}
# 采集包名
COLLECT_JAR_NAME="hyren-serv6-m-${RELEASE_VERSION}.jar"
# 采集程序Main方法
MAIN_CLASS="hyren.serv6.m.main.MetaCollTaskMain"
# HADOOP_OPTS
HADOOP_OPTS="-Djava.library.path=/opt/cloudera/parcels/CDH/lib/hadoop/lib/native"
# Get system bits
OS_BIT=$(getconf LONG_BIT)
# 脚本标签
#COLLECT_TAG="SPDBANK_WRITEDDL_JOB"
# 批次ID
TASK_ID="${1}"
# 截取参数,只取需要传递给java程序的参数
# shellcheck disable=SC2124
PARAM_STR="${@}"
PARAM_STR="${PARAM_STR%-P hyrenJob -J*}"
# SQL占位参数,截取并处理参数中的SQL占位符
SQL_PARAMS=""
# shellcheck disable=SC2206
PARAMS=(${PARAM_STR})
for ((i = 2; i < ${#PARAMS[@]}; i++)); do SQL_PARAMS="${PARAMS[i]} ${SQL_PARAMS}"; done

# 脚本运行入口
## 参数1：批次ID
## 参数2：表名
## 参数3：跑批日期
## 参数4-N：sql占位符参数 String[]{condition=value}
## 使用方式 sh shellCommand.sh 899948182551662592 agent_info 20211018 -PhyrenJob -Jdtest_pg_agent_info_PARQUET
## 使用方式 sh shellCommand.sh 899948182551662592 agent_info 20211018 condition=value -PhyrenJob -Jdtest_pg_agent_info_PARQUET
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
  CLASSPATH=".:${SH_EXEC_DIR}/resources:${SH_EXEC_DIR}/${COLLECT_JAR_NAME}:"
#  CLASSPATH="${CLASSPATH}${SH_EXEC_DIR}/../lib/*.jar"
  CLASSPATH="${CLASSPATH}${SH_EXEC_DIR}/../jdbc/*"

    java -Xms64m -Xmx1024m ${HADOOP_OPTS} \
    -Dproject.name="${COLLECT_TAG}" \
    -Dproject.id="${TASK_ID}" \
    -DisMultiLineLog=true \
    -Djava.ext.dirs="${SH_EXEC_DIR}"/../jdbc:"${SH_EXEC_DIR}"/../jre/linux/"${OS_BIT}"/jre/lib/ext \
    -cp "${CLASSPATH}" -Dloader.main="${MAIN_CLASS}"  org.springframework.boot.loader.PropertiesLauncher "${TASK_ID}"
    
}

#function usage means how to use this script.
function usage() {
  echo "Usage: $0 TASK_ID"
  echo 'for example: m-job.sh "899948182551662592"'
  exit
}

# 加载脚本
main "$@"

