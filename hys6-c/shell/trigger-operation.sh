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
AGENT_JAR_NAME="hyren-serv6-trigger-${RELEASE_VERSION}.jar"
# Trigger程序Main方法
MAIN_CLASS="hyren.serv6.trigger.AppMain"
# maxFormContentSize
MAX_FORM_CONTENT_SIZE="-1"
# agent tag
AGENT_TAG="HYREN_TRIGGER"
# SYS_CODE
SYS_CODE="${1}"
# agent mode of operation
ETL_DATE="${2}"
# YEAR MONTH DAY
YEAR=${ETL_DATE:0:4}
MONTH=${ETL_DATE:0:6}
DAY=${ETL_DATE:0:8}
LOG_YEAR_DIR=${SH_EXEC_DIR}"/"${YEAR}
LOG_MONTH_DIR=${LOG_YEAR_DIR}"/"${MONTH}
# agent mode of operation
AGENT_OPERATE="${3}"

# 脚本运行入口
## 参数1  SYS_CODE 工程编号
## 使用方式 sh trigger-operation.sh S01 restart
## 返回状态码说明 {0: 启动成功; 1: 程序包不存在; 2: 不支持的操作类型; 3: 服务已经启动; 4: Jre文件不存在; 255: 启动失败}
main() {

  # if no parameter is passed to script then show how to use.
  if [[ $# -eq 0 ]]; then usage; fi
  if [[ $# -ne 3 ]]; then usage; fi
  # Enter the script directory
  cd "${SH_EXEC_DIR}" || exit
  # file add execute permission
  if [[ -d ${DEPLOYMENT_DIR} ]]; then chmod -R 755 "${DEPLOYMENT_DIR}"; fi
  # Check the legality of the file
  if [[ ! -f ${AGENT_JAR_NAME} ]]; then echo "Agent service package file: ${AGENT_JAR_NAME} does not exist, please check !" && exit 1; fi
  # Log directory initialization
  ## Create the directory where the log configuration is located
  if [[ ! -d ${LOG_MONTH_DIR} ]]; then mkdir -p "${LOG_MONTH_DIR}"; fi
  #标准输出日志
  LOG_OUT_FILE="${LOG_MONTH_DIR}/${DAY}_TriggerOut.log"
  echo "标准输出日志: ${LOG_OUT_FILE}"
  #错误输出日志
  LOG_ERR_FILE="${LOG_MONTH_DIR}/${DAY}_TriggerErr.log"
  echo "标准输出错误日志: ${LOG_ERR_FILE}"
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
    echo "Unsupported operation type. see start or stop or restart."
    echo_done " "
    exit 2
  fi
}

# Start agent service
function start_agent() {
  # Get the agent_pid of the specified
  AGENT_PID=$(pgrep -af ${AGENT_TAG} |
    grep "${SH_EXEC_DIR}" |
    grep -v grep |
    awk '{print $1}' | xargs -n 1)
  if [[ -n "${AGENT_PID}" ]]; then
    echo "The agent service is already startup on ${SH_EXEC_DIR}."
    echo_done " "
    exit 3
  else
    # The agent service is not running, start it.
    ## Enter the script directory
    cd "${DEPLOYMENT_DIR}" || exit
    ## Check jre environment
    if [[ ! -d "${DEPLOYMENT_DIR}/jre" ]]; then
      echo "jre does not exist, please check the jre env."
      echo_done " "
      exit 4
    fi
    ## Start the agent service
    ### 查看加载类信息 -verbose:class
    ### -Dorg.eclipse.jetty.server.Request.maxFormContentSize=${MAX_FORM_CONTENT_SIZE} \
    nohup "${DEPLOYMENT_DIR}"/jre/linux/"${OS_BIT}"/jre/bin/java -Xms64m -Xmx1024m \
      -Dproject.name="${AGENT_TAG}" \
      -Dproject.dir="${SH_EXEC_DIR}" \
      -Djava.ext.dirs="${SH_EXEC_DIR}/../jdbc:${SH_EXEC_DIR}/../jars:${SH_EXEC_DIR}/jre/linux/${OS_BIT}/jre/lib/ext" \
      -DbasePackage=trigger -DprojectId=trigger -DisMultiLineLog=true \
      -Xbootclasspath/a:"${SH_EXEC_DIR}/resources" \
      -jar "${SH_EXEC_DIR}/${AGENT_JAR_NAME}" \
      sys.code="${SYS_CODE}" 1>>"${LOG_OUT_FILE}" 2>>"${LOG_ERR_FILE}" &
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
      echo "The agent service is Successful start."
      echo_done " "
      exit 0
    else
      echo "The agent service did not start successfully, please contact the administrator."
      echo_done " "
      exit 255
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
    echo_done
    echo "The agent service is Successful stop."
  else
    echo "The agent service is not running, ignore."
    echo_done " "
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
  echo "Usage: $0 调度系统代码 跑批日期 start"
  echo "for example: $0 S01 20230522 start"
  exit
}

# 第1个参数：服务
# 第2个参数：提示信息
# 第3个参数：0-等待启动成功，1-等待停止成功
waiting_proc_status() {
  echo_doing "Waiting [$1] for $2 ..."
  local nums=0
  while true; do
    pgrep -af ${AGENT_TAG} | grep "${SH_EXEC_DIR}" | grep -v grep >/dev/null 2>&1
    if [[ $? -ne $3 ]]; then
      nums=$((nums + 1))
      if [[ ${nums} -gt 10 ]]; then
        echo "$2 failed, please check the log information."
        exit 1
      else
        sleep 1
        echo_doing "."
      fi
    else
      sleep 1
      echo_doing "."
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
echo_done " "

# 加载脚本
main "$@"
