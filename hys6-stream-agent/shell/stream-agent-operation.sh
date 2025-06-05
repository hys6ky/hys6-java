#!/bin/bash

# agent deployment directory
AGENT_DEPLOYMENT_DIR=$(dirname "$(pwd)")
# shell script execution directory
SH_EXEC_DIR=$(
  cd "$(dirname "$0")" || exit
  pwd
)
# Get system bits
OS_BIT=$(getconf LONG_BIT)
# agent service package name
AGENT_JAR_NAME="${1}"
# agent log full file
AGENT_LOG_FILE="${2}"
# maxFormContentSize
MAX_FORM_CONTENT_SIZE=-1
# agent tag
AGENT_TAG="HYREN_STREAM_AGENT"
# agent port
AGENT_PORT="${3}"
# agent mode of operation
AGENT_OPERATE="${4}"

# 脚本运行入口
## 参数1  Agent 程序包名 hyren-serv6-stream-agent-6.0.jar
## 参数2  Agent 程序运行日志文件路径 /home/hyshf/agent/agent_log_dir/log.out OR /home/hyshf/agent/agent_log_dir/
## 参数3  Agent 程序运行绑定端口 34561
## 参数4  Agent 服务操作类型 start 或者 stop 或者 restart
## 返回状态码说明:
### { 0: 启动成功; 1: 程序包不存在; 2: 不支持的操作类型; 3: 服务已经启动; 4: Jre文件不存在; 255: 启动失败; 99: 参数不合法; }
main() {
  # if no parameter is passed to script then show how to use.
  if [[ $# -eq 0 ]]; then usage; fi
  if [[ $# -ne 4 ]]; then usage; fi
  # Enter the script directory
  cd "${SH_EXEC_DIR}" || exit
  # file add execute permission
  if [[ -d ${AGENT_DEPLOYMENT_DIR} ]]; then chmod -R 755 "${AGENT_DEPLOYMENT_DIR}"; fi
  # Check the legality of the file
  if [[ ! -f ${AGENT_JAR_NAME} ]]; then echo "Agent service package file does not exist, please check !" && exit 1; fi
  # Log directory initialization
  ## Create the directory where the log configuration is located
  AGENT_LOG_DIR=${AGENT_LOG_FILE%/*}
  if [[ ! -d ${AGENT_LOG_DIR} ]]; then mkdir -p "${AGENT_LOG_DIR}"; fi
  # Check port legality
  if [[ -z ${AGENT_PORT} ]]; then echo "Port is empty, please check port legality !"; fi
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
    echo "Unsupported operation type! @see {start: 启动, stop: 停止, restart: 重启}"
    exit 2
  fi
}

# Stop agent service
function stop_agent() {
  echo "AAAAAAAAAA ${AGENT_TAG} ${AGENT_DEPLOYMENT_DIR} Dport=${AGENT_PORT}"
  # Get the agent_pid of the specified
  AGENT_PID=$(pgrep -af ${AGENT_TAG} |
    grep "${AGENT_DEPLOYMENT_DIR}" |
    grep "Dport=${AGENT_PORT}" |
    grep -v grep |
    awk '{print $1}' | xargs -n 1)
  if [[ -n "${AGENT_PID}" ]]; then
    # The agent service already exists and starts, stop the agent service
    kill -15 "${AGENT_PID}"
    # Wait for the port to stop
    waiting_proc_status "${AGENT_PORT}" "stop" 1
    echo_done
    echo "The agent service is successful stop."
  else
    echo "The agent service is not running, ignore."
  fi
}

# Start agent service
function start_agent() {
  # Get the agent_pid of the specified
  AGENT_PID=$(pgrep -af ${AGENT_TAG} |
    grep "${AGENT_DEPLOYMENT_DIR}" |
    grep "Dport=${AGENT_PORT}" |
    grep -v grep |
    awk '{print $1}' | xargs -n 1)
  if [[ -n "${AGENT_PID}" ]]; then
    echo "The agent service is already startup on ${AGENT_DEPLOYMENT_DIR}, its port is ${AGENT_PORT}."
    exit 3
  else
    # The agent service is not running, start it.
    ## Load user operating environment
    #source ~/.bash_profile; source ~/.bashrc;
    ## Enter the script directory
    cd "${SH_EXEC_DIR}" || exit
    ## Check jre environment
    if [[ ! -d "${SH_EXEC_DIR}/jre" ]]; then
      echo "jre does not exist, please check the jre env."
      exit 4
    fi
    ## Start the agent service
    ### 查看加载类信息 -verbose:class
    ### -Dorg.eclipse.jetty.server.Request.maxFormContentSize=${MAX_FORM_CONTENT_SIZE} \
    nohup "${SH_EXEC_DIR}"/jre/linux/"${OS_BIT}"/jre/bin/java \
      -Dproject.name="${AGENT_TAG}" \
      -Dport="${AGENT_PORT}" \
      -Dproject.dir="${AGENT_DEPLOYMENT_DIR}" \
      -DisMultiLineLog=true \
      -Djava.ext.dirs="${AGENT_DEPLOYMENT_DIR}"/jdbc:"${SH_EXEC_DIR}"/jre/linux/${OS_BIT}/jre/lib/ext \
      -Xbootclasspath/a:./resources \
      -jar "${AGENT_JAR_NAME}" >/dev/null 2>&1 &
    # After the startup is executed, check agent service startup status
    ## Wait for the port to start
    waiting_proc_status "${AGENT_PORT}" "start" 0
    sleep 1
    ## Check the agent service startup log
    check_startup_log "${AGENT_PORT}"
    ## Get the agent_pid of the specified
    AGENT_PID=$(pgrep -af ${AGENT_TAG} |
      grep "${AGENT_DEPLOYMENT_DIR}" |
      grep Dport="${AGENT_PORT}" |
      grep -v grep |
      awk '{print $1}' | xargs -n 1)
    if [[ -n "${AGENT_PID}" ]]; then
      echo "The agent service is Successful start."
      exit 0
    else
      echo "The agent service did not start successfully, please contact the administrator."
      exit 255
    fi
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
  echo "Usage: $0 AGENT_JAR_NAME AGENT_LOG_DIR AGENT_PORT OPERATE"
  echo "for example: bash $0 hyren-serv6-stream-agent-6.0.jar /agent_deploy_dir/hrsagent/streamAgent_20099/running/running.log 32101 start"
  exit
}

# 第1个参数：端口
# 第2个参数：提示信息
# 第3个参数：0-等待启动成功，1-等待停止成功
waiting_proc_status() {
  echo_doing "Waiting [$1] for $2 ..."
  local nums=0
  while true; do
    ss -tnl | grep -w "$1" >/dev/null 2>&1
    if [[ $? -ne $3 ]]; then
      nums=$((nums + 1))
      if [[ ${nums} -gt 30 ]]; then
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

# 检查 agent 服务启动日志
# 第1个参数：端口
check_startup_log() {
  local nums=0
  while true; do
    # 有这一行表明已经启动成功
    if grep "Tomcat started on port(s): $1" "${AGENT_LOG_FILE}" >/dev/null; then
      echo_done
      break
    else
      # 循环10秒检查启动成功标志
      nums=$((nums + 1))
      if [[ ${nums} -gt 30 ]]; then
        echo "Startup failed, please check the log: ${AGENT_LOG_FILE}"
        exit 1
      else
        sleep 1
        echo_doing "."
      fi
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
echo_done "$@"

# 加载脚本
main "$@"
