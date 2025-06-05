#!/bin/sh

cd `dirname $0`


export ENGINE=hyren-serv6-stream-agent-6.0.jar
export MAIN=hyren.serv6.stream.job.CommandExecut
export CLASSPATH=$CLASSPATH:$ENGINE:config/:

export PROJECTDIR=.

function libjars(){
for file in $PROJECTDIR/lib/*.jar
do
if [ -f $file ]
then
 #echo $file
 CLASSPATH="$CLASSPATH$file:"
fi
done
}

libjars
export CLASSPATH
echo $CLASSPATH
java -Xms512m -Xmx1024m -Dproject.dir="$PROJECTDIR" -cp $CLASSPATH $MAIN $1
