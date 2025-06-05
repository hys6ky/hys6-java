#!/bin/bash

#包发布的版本号
RELEASE_VERSION="6.1"
#通过该脚本调用数据分发作业（调度平台调用）
# shell script execution directory
SH_EXEC_DIR=$(cd $(dirname $0); pwd)
# 采集包名
COLLECT_JAR_NAME="hyren-serv6-b-${RELEASE_VERSION}.jar"
# 数据分发程序Main方法
MAIN_CLASS="hyren.serv6.b.datadistribution.run.DistributeJobCommand"
# HADOOP_OPTS
HADOOP_OPTS="-Djava.library.path=/opt/cloudera/parcels/CDH/lib/hadoop/lib/native"
# Get system bits
OS_BIT=`getconf LONG_BIT`
# 脚本标签
DISTRIBUTE_TAG="DATA_DISTRIBUTE"
# 数据分发主键id
DD_ID="${1}"
shift
# 跑批日期
CURR_BATH_DATE="${1}"
shift
# SQL占位参数,截取并处理参数中的SQL占位符
for ((i=0; i<$#; i++)); do
  if [ "${1}" != "-P" ]; then SQL_PARAMS="$SQL_PARAMS ${1}"; shift; fi
done

# 脚本运行入口
## 参数1：数据分发主键id
## 参数2：跑批日期
## 参数3-N：sql占位符参数 String[]{condition=value}
## 使用方式 sh shellCommand.sh "899948182551662592" "20211001"
## 使用方式 sh shellCommand.sh "899948182551662592" "20211001" "condition=value" -PhyrenJob -Jdtest_pg_agent_info_PARQUET
## 返回状态码说明 {1: 程序包不存在}
main(){
    # if no parameter is passed to script then show how to use.
    if [[ $# -eq 0 ]]; then usage; fi
    # Enter the script directory
    cd ${SH_EXEC_DIR}
    # Check the legality of the file
    if [[ ! -f ${COLLECT_JAR_NAME} ]]; then echo "Collect service package file does not exist, please check !" && exit 1; fi
    # execute script
    distribute_main
}

# 通过该脚本调用数据分发作业（调度平台调用）
function distribute_main() {
    # Configure classpath
    CLASSPATH=".:${SH_EXEC_DIR}/resources:${SH_EXEC_DIR}/${COLLECT_JAR_NAME}:"
    CLASSPATH="${CLASSPATH}${SH_EXEC_DIR}/jars/*.jar"
    CLASSPATH="${CLASSPATH}${SH_EXEC_DIR}/jdbc/*"
    # echo "${DD_ID} ${SQL_PARAMS}"
    java -Xms64m -Xmx1024m ${HADOOP_OPTS} \
        -Dproject.name="${DISTRIBUTE_TAG}" \
        -Dproject.id="${DD_ID}" \
        -Djava.ext.dirs=${SH_EXEC_DIR}/../jdbc:${SH_EXEC_DIR}/../jre/linux/${OS_BIT}/jre/lib/ext \
        -DbasePackage=agent \
        -DisMultiLineLog=true \
        -Xbootclasspath/a:./resources \
        -cp "${CLASSPATH}" -Dloader.main="${MAIN_CLASS}" org.springframework.boot.loader.PropertiesLauncher \
        ${DD_ID} ${CURR_BATH_DATE} ${SQL_PARAMS}
}

#function usage means how to use this script.
function usage(){
    # shellcheck disable=SC2016
    echo 'Usage: $0 DD_ID CURR_BATH_DATE SQL_PARAMS'
    # shellcheck disable=SC2016
    echo 'for example: $0 "899948182551662592" "20211001"'
    echo 'OR'
    # shellcheck disable=SC2016
    echo 'for example: $0 "899948182551662592" "20211001" "condition=value"'
    exit
}
# 加载脚本
main "$@"
