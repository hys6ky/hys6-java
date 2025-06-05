#!/bin/bash

#kinit -k -t /wmdpapp/cdh_clinet/bdpmt_aflt_swm_etl.keytab bdpmt_aflt_swm_etl@MCIPT.COM
#包发布的版本号
RELEASE_VERSION="6.1"
#通过该脚本调用加工作业（调度平台调用）
# shell script execution directory
SH_EXEC_DIR=$(
  cd "$(dirname "$0")" || exit
  pwd
)
# 加工包名
PROCESS_JAR_NAME="hyren-serv6-h-process.jar"
# 加工程序Main方法
MAIN_CLASS="hyren.serv6.h.process.run.ProcessMainJob"
# HADOOP_OPTS
#HADOOP_OPTS="-Djava.library.path=/opt/cloudera/parcels/CDH/lib/hadoop/lib/native"
# Get system bits
OS_BIT=$(getconf LONG_BIT)
# 脚本标签
JOB_TAG="PROCESS_JOB"
# 模型表id_作业id
MODULE_TABLE_ID="${1}"
shift
# 模型表id_作业id
JOB_TABLE_ID="${1}"
shift
# 跑批日期
ETL_DATE="${1}"
shift
#截取并处理参数中的SQL占位符
for ((i = 0; i < $#; i++)); do
  if [ "${1}" != "-P" ]; then
    SQL_PARAMS="$SQL_PARAMS ${1}"
    shift
  fi
done

#function usage means how to use this script.
function usage() {
  echo
  echo "Usage       : bash $(basename $0) MODULE_TABLE_ID JOB_TABLE_ID ETL_DATE [Options]"
  echo "For example : bash $(basename $0) 961942930786906112 961942930786906113 20230601 -P hyrenJob -J job1 -S dataMarket -E hll -D"
  echo
  echo "Warning: Script parameters must precede optional parameters!"
  echo
  echo "Options :"
  echo "    -P  <JOB_FLAG>   : 海云作业标识"
  echo "    -J  <JOB_NAME>   : 作业名"
  echo "    -S  <JOB_SOURCE> : 作业标签"
  echo "    -E  <ETL_SYS_CD> : 调度工程编号"
  echo "    -D  <ARGS_DEBUG> : 参数DEBUG模式,无参数,默认：TRUE,输出参数信息"
  echo
  if [ "Null$1" != "Null" ]; then
    echo "$1"
    echo
  fi
  exit 901
}

#选项后面的冒号表示该选项需要参数
while getopts P:J:S:E:D arg_opt; do
  case ${arg_opt} in
  P)
    JOB_FLAG="$OPTARG"
    ;;
  J)
    JOB_NAME="$OPTARG"
    ;;
  S)
    JOB_SOURCE="$OPTARG"
    ;;
  E)
    ETL_SYS_CD="$OPTARG"
    ;;
  D)
    ARG_DEBUG="TRUE"
    ;;
  ?)
    usage
    ;;
  esac
done

# 参数检查
[ "Null${MODULE_TABLE_ID}" == "Null" ] && usage "ERROR: Missing parameter:1 <MODULE_TABLE_ID>"
[ "Null${JOB_TABLE_ID}" == "Null" ] && usage "ERROR: Missing parameter:2 <JOB_TABLE_ID>"
[ "Null$ETL_DATE" == "Null" ] && usage "ERROR: Missing parameter:3 <ETL_DATE>"

# 回显命令行参数值
if [ "$ARG_DEBUG" == "TRUE" ]; then
  echo
  echo "============================================="
  echo "MODULE_TABLE_ID : ${MODULE_TABLE_ID}"
  echo "JOB_TABLE_ID    : ${JOB_TABLE_ID}"
  echo "JOB_FLAG        : ${JOB_FLAG}"
  echo "JOB_NAME        : ${JOB_NAME}"
  echo "JOB_SOURCE      : ${JOB_SOURCE}"
  echo "ETL_SYS_CD      : ${ETL_SYS_CD}"
  echo "============================================="
  echo
fi

# 脚本运行入口
## 参数1：模型表id
## 参数2：作业表id
## 参数3：跑批日期
## 使用方式 sh process-job-command.sh 1095668621008498688 1095668622723969024 20230416
## 返回状态码说明 {900: 程序包不存在, 901: 参数不合法}
main() {
  # if no parameter is passed to script then show how to use.
  if [[ $# -eq 0 ]]; then usage; fi
  # Enter the script directory
  cd "${SH_EXEC_DIR}" || exit
  # Check the legality of the file
  if [[ ! -f ${PROCESS_JAR_NAME} ]]; then
    echo "Service package file [ ${PROCESS_JAR_NAME} ] does not exist, please check !" && exit 900
  fi
  # execute script
  collect_main
}

# 通过该脚本调用加工作业（调度平台调用）
function collect_main() {
  # Configure classpath
  CLASSPATH=".:${SH_EXEC_DIR}/resources:${SH_EXEC_DIR}/${PROCESS_JAR_NAME}:"
  CLASSPATH="${CLASSPATH}${SH_EXEC_DIR}/process-run-jars/*:"
  CLASSPATH="${CLASSPATH}${SH_EXEC_DIR}/../jdbc/*"
  # 如果参数是以空格分隔的多个参数,不能使用 "" 包围传递
  echo ${PROCESS_JAR_NAME} ${MODULE_TABLE_ID} ${JOB_TABLE_ID} ${ETL_DATE} ${SQL_PARAMS}
  "${SH_EXEC_DIR}"/../jre/linux/"${OS_BIT}"/jre/bin/java -Xms64m -Xmx1024m ${HADOOP_OPTS} \
    -DJOB_TAG="${JOB_TAG}" \
    -DMODULE_TABLE_ID="${MODULE_TABLE_ID}" -DJOB_TABLE_ID="${JOB_TABLE_ID}" \
    -Djava.ext.dirs="${SH_EXEC_DIR}"/../jdbc:"${SH_EXEC_DIR}"/jre/linux/"${OS_BIT}"/jre/lib/ext \
    -DisMultiLineLog=true \
    -DbasePackage=process \
    -DprojectId=process \
    -Xbootclasspath/a:./resources \
    -cp ${CLASSPATH} ${MAIN_CLASS} \
    ${MODULE_TABLE_ID} ${JOB_TABLE_ID} ${ETL_DATE} ${JOB_NAME} ${SQL_PARAMS}
}

# 加载脚本
main "$@"
