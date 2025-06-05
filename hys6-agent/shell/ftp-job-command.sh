#!/bin/bash

#包发布的版本号
RELEASE_VERSION="6.1"
#通过该脚本调用集市作业（调度平台调用）
# shell script execution directory
SH_EXEC_DIR=$(
  cd "$(dirname "$0")" || exit
  pwd
)
COLLECT_JAR_NAME="hyren-serv6-agent-${RELEASE_VERSION}.jar"
# 采集程序Main方法
MAIN_CLASS="hyren.serv6.agent.run.FtpJobCommand"
# HADOOP_OPTS
HADOOP_OPTS="-Djava.library.path=/opt/cloudera/parcels/CDH/lib/hadoop/lib/native"
# Get system bits
OS_BIT=$(getconf LONG_BIT)
# 脚本标签
COLLECT_TAG="HYREN_FTP_JOB"
# 采集任务id
TASK_ID="${1}"
shift

# 脚本运行入口
## 参数1：任务ID
## 参数2：表名
## 参数3：采集类型
## 参数4：跑批日期
## 参数5：文件格式或存储目的地名称
## 参数6-N：sql占位符参数 String[]{condition=value}
## 使用方式 sh collect-job-command.sh 899948182551662592 agent_info 1 20211018 4 -PhyrenJob -Jdtest_pg_agent_info_PARQUET
## 使用方式 sh collect-job-command.sh 899948182551662592 agent_info 1 20211018 4 condition=value -PhyrenJob -Jdtest_pg_agent_info_PARQUET
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
    -Dproject.id="${TASK_ID}" \
    -DbasePackage=agent \
    -DisMultiLineLog=true \
    -Djava.ext.dirs="${SH_EXEC_DIR}/../jdbc:${SH_EXEC_DIR}/../jars:${SH_EXEC_DIR}/jre/linux/${OS_BIT}/jre/lib/ext" \
    -Xbootclasspath/a:./resources \
    -cp "${CLASSPATH}" -Dloader.main="${MAIN_CLASS}" org.springframework.boot.loader.PropertiesLauncher \
    "${TASK_ID}"
}

#function usage means how to use this script.
function usage() {
  echo "Usage: $0 TASK_ID"
  echo 'for example: collect-job-command.sh "899948182551662592"'
  exit
}

# 加载脚本
main "$@"
