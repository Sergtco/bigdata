#!/bin/bash
export HADOOP_JARS=$HADOOP_HOME/share/hadoop
javac -cp $HADOOP_JARS/common/hadoop-common-3.4.2.jar:\
$HADOOP_JARS/mapreduce/hadoop-mapreduce-client-core-3.4.2.jar $1
bn=`basename $1 .java`
jar cf $bn.jar $bn*.class
