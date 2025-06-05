#!/bin/bash

export LANG='zh_CN.UTF-8'

#包发布的版本号
RELEASE_VERSION="6.1"
# deployment directory
DEPLOYMENT_DIR=$(dirname "$(pwd)")
# shell script execution directory
SH_EXEC_DIR=$(
  cd "$(dirname "$0")" || exit
  pwd
)
# Get system bits
OS_BIT=$(getconf LONG_BIT)
# agent service package name
AGENT_JAR_NAME="hyren-serv6-control-${RELEASE_VERSION}.jar"
# Control程序Main方法
MAIN_CLASS="hyren.serv6.control.AppMain"
# maxFormContentSize
MAX_FORM_CONTENT_SIZE="-1"
# agent tag
AGENT_TAG="HYREN_CONTROL"
# ETL_DATE
ETL_DATE="${1}"
# YEAR MONTH DAY
YEAR=${ETL_DATE:0:4}
MONTH=${ETL_DATE:0:6}
DAY=${ETL_DATE:0:8}
LOG_YEAR_DIR=${SH_EXEC_DIR}"/"${YEAR}
LOG_DIR=${LOG_YEAR_DIR}"/"${MONTH}
# SYS_CODE
SYS_CODE="${2}"
# CR
CR="${3}"
# AS
AS="${4}"
# END_DATE
END_DATE=${5}
# agent mode of operation
AGENT_OPERATE="${6}"
# 作业ID占位参数,截取并处理参数中的作业ID占位符
JOB_ID_PARAMS="${7}"

# 脚本运行入口
## 参数1  ETL_DATE 调度日期
## 参数2  SYS_CODE 工程编号
## 参数3  CR 是否许跑 1: 是 0: 否
## 参数4  AS 是否日切 1: 是 0: 否
## 参数5  start 或者 stop 或者 restart
## 使用方式 bash control-operation.sh 20230101 hll 0 0 99991231 restart
##      OR bash control-operation.sh 20230101 hll 0 0 99991231 restart 进程id
## 返回状态码说明 {
###   0: 启动成功; 1: 程序包不存在; 2: 不支持的操作类型; 3: 服务已经启动; 4: jre文件不存在;
###   5: Control停止成功; 6: Control服务未运行; 7: 当前工程还存在子任务正在运行, 正在运行的作业数: number;
###   8: 当前作业正在运行 JOB_ID ; 255: 启动失败;
### }
main() {
  # if no parameter is passed to script then show how to use.
  if [[ $# -eq 0 ]]; then usage; fi
  if [[ $# -lt 5 ]]; then usage; fi
  # Enter the script directory
  cd "${SH_EXEC_DIR}" || exit
  # file add execute permission
  if [[ -d ${DEPLOYMENT_DIR} ]]; then chmod -R 755 "${DEPLOYMENT_DIR}"; fi
  # Check the legality of the file
  # if [[ ! -f ${AGENT_JAR_NAME} ]]; then echo "Agent service package file:  ${AGENT_JAR_NAME} does not exist, please check !" && exit 1; fi
  if [[ ! -f ${AGENT_JAR_NAME} ]]; then echo "1: 程序包不存在" && exit 1; fi
  # Log directory initialization
  ## Create the directory where the log configuration is located
  if [[ ! -d ${LOG_DIR} ]]; then mkdir -p "${LOG_DIR}"; fi
  # 标准输出日志
  LOG_OUT_FILE="${LOG_DIR}/${DAY}_ControlOut.log"
  # echo "标准输出日志: ${LOG_OUT_FILE}"
  # 错误输出日志
  LOG_ERR_FILE="${LOG_DIR}/${DAY}_ControlErr.log"
  # echo "标准输出错误日志: ${LOG_ERR_FILE}"
  # execute script
  agent_main
}

# script main
function agent_main() {
  if [[ ${AGENT_OPERATE} == "start" ]]; then
    # Start the agent process.
    start_agent
  elif [[ ${AGENT_OPERATE} == "stop" ]]; then
    # Stop the agent process.
    stop_agent
  elif [[ ${AGENT_OPERATE} == "restart" ]]; then
    # Restart the agent process.
    restart_agent
  else
    # echo "Unsupported operation type. see start or stop or restart."
    echo "2: 不支持的操作类型"
    exit 2
  fi
}

# Start agent service
function start_agent() {
  # Check if the current job id is still running
  JOB_ID_PARAMS_ARR=("${JOB_ID_PARAMS//,/ }")
  # shellcheck disable=SC2068
  local JOB_ID_COUNT=0
  local JOB_ID_STR=""
  for JOB_ID in ${JOB_ID_PARAMS_ARR[@]}; do
    # shellcheck disable=SC2009
    JOB_NUM=$(ps -ef | grep -v grep | grep -w "${JOB_ID}" | awk '{print $2}' | grep -w "${JOB_ID}" | grep -c "${JOB_ID}")
    if [[ "${JOB_NUM}" -ne 0 ]]; then
      #echo "8: 当前作业正在运行 JOB_ID是 ${JOB_ID}"
      #exit 8
      let JOB_ID_COUNT=JOB_ID_COUNT+1
      JOB_ID_STR=${JOB_ID_STR}${JOB_ID}"|"
    fi
  done
  # 工程下还存在作业正在运行
  if [[ "${JOB_ID_COUNT}" -ne 0 ]]; then
    echo "9: 该工程下存在[ ${JOB_ID_COUNT} ]个进程未运行完成，进程ID为[ ${JOB_ID_STR%?} ],请等相关进程运行完成或手动后台结束相关进程。}"
    exit 9
  fi
  # Get the current Control child process
  JOB_NUMS=$(pgrep -af "hyrenJob" | grep -w "PC${SYS_CODE}" | grep -c "PC${SYS_CODE}")
  if [[ "${JOB_NUMS}" -ne 0 ]]; then
    echo "7: 当前工程还存在子任务正在运行, 正在运行的作业数 ${JOB_NUMS}"
    exit 7
  else
    # Get the agent_pid of the specified
    AGENT_PID=$(pgrep -af ${AGENT_TAG} |
      grep "${SH_EXEC_DIR}" |
      grep -v grep |
      awk '{print $1}' | xargs -n 1)
    if [[ -n "${AGENT_PID}" ]]; then
      # echo "The agent service is already startup on ${SH_EXEC_DIR}."
      echo "3: 服务已经启动"
      exit 3
    else
      # The agent service is not running, start it.
      ## Enter the script directory
      cd "${DEPLOYMENT_DIR}" || exit
      ## Check jre environment
      if [[ ! -d "${DEPLOYMENT_DIR}/jre" ]]; then
        # echo "jre does not exist, please check the jre env."
        echo "4: Jre文件不存在"
        exit 4
      fi
      ## Start the agent service
      ### 查看加载类信息 -verbose:class
      ### -Dorg.eclipse.jetty.server.Request.maxFormContentSize=${MAX_FORM_CONTENT_SIZE} \
      nohup "${SH_EXEC_DIR}/../jre/linux/${OS_BIT}"/jre/bin/java -Xms64m -Xmx1024m \
        -Dproject.name="${AGENT_TAG}" \
        -Dproject.dir="${SH_EXEC_DIR}" \
        -Djava.ext.dirs="${SH_EXEC_DIR}/../jdbc:${SH_EXEC_DIR}/../jars:${SH_EXEC_DIR}/jre/linux/${OS_BIT}/jre/lib/ext" \
        -DbasePackage=control -DprojectId=control -DisMultiLineLog=true \
        -Xbootclasspath/a:"${SH_EXEC_DIR}/resources" \
        -jar "${SH_EXEC_DIR}/${AGENT_JAR_NAME}" \
        etl.date="${ETL_DATE}" sys.code="${SYS_CODE}" -CR="${CR}" -AS="${AS}" end.date="${END_DATE}" \
        1>>"${LOG_OUT_FILE}" 2>>"${LOG_ERR_FILE}" &
      # After the startup is executed, check agent service startup status
      ## Wait for the port to start
      waiting_proc_status "${SH_EXEC_DIR}" "start" 0
      sleep 1
      ## Get the agent_pid of the specified
      AGENT_PID=$(pgrep -af ${AGENT_TAG} |
        grep -w "${SH_EXEC_DIR}" |
        grep -v grep |
        awk '{print $1}' | xargs -n 1)
      if [[ -n "${AGENT_PID}" ]]; then
        # echo "The agent service is Successful start."
        echo "0: 启动成功"
        exit 0
      else
        # echo "The agent service did not start successfully, please contact the administrator."
        echo "255: 启动失败"
        exit 255
      fi
    fi
  fi
}

# Stop agent service
function stop_agent() {
  # Get the agent_pid of the specified
  AGENT_PID=$(pgrep -af ${AGENT_TAG} |
    grep -w "${SH_EXEC_DIR}" |
    grep -v grep |
    awk '{print $1}' | xargs -n 1)
  if [[ -n "${AGENT_PID}" ]]; then
    # The agent service already exists and starts, stop the agent service
    kill -15 "${AGENT_PID}"
    # Wait for the port to stop
    waiting_proc_status "${SH_EXEC_DIR}" "stop" 1
    # echo "The agent service is Successful stop."
    echo "5: Control停止成功"
    return 5
  else
    # echo "The agent service is not running, ignore."
    echo "6: Control服务未运行"
    return 6
  fi
}

# Restart agent service
function restart_agent() {
  # stop agent service
  stop_agent
  # start agent service
  start_agent
}

#function usage means how to use this script.
function usage() {
  echo "Usage: $0 跑批日期（格式yyyyMMDD） 调度系统代码 是否为续跑(0：是，1：否） 是否自动日切(0：是，1：否）日切结束日期(日切标识为: 1 ,需要指定日切结束日期, 日切标识为: 0 ,默认传入 99991231) 操作类型（start：启动，stop：停止，restart：重启）"
  echo "for example: $0 20211001 S01 0 0 20230801 restart OR $0 20211001 S01 0 0 20230801 restart 进程id"
  exit
}

# 第1个参数：服务
# 第2个参数：提示信息
# 第3个参数：0-等待启动成功，1-等待停止成功
waiting_proc_status() {
  #	echo_doing "Waiting [$1] for $2 ..."
  local nums=0
  while true; do
    pgrep -af ${AGENT_TAG} | grep "${SH_EXEC_DIR}" | grep -v grep >/dev/null 2>&1
    if [[ $? -ne $3 ]]; then
      nums=$((nums + 1))
      if [[ ${nums} -gt 10 ]]; then
        # echo "$2 failed, please check the log information."
        exit 1
      else
        sleep 1
        # echo_doing "."
      fi
    else
      sleep 1
      # echo_doing "."
      break
    fi
  done
}

echo_doing() {
  if [[ "x$1" == "x" ]]; then
    echo -ne "install doing ..."
  else
    echo -ne "$1"
  fi
}

echo_done() {
  if [[ "x$1" == "x" ]]; then
    echo " done"
  else
    echo " $1"
  fi
}

# 加载脚本
main "$@"
