#!/bin/bash
./hdpCompile.sh ./CrossCorrelationStripes.java
hadoop fs -mkdir -p /ccs/input
hadoop fs -put -p ./order_db.txt /ccs/input/order_db.txt
hadoop jar CrossCorrelationStripes.jar CrossCorrelationStripes /ccs/input /ccs/output 
hadoop fs -copyToLocal /ccs/output ./ccs-output
hadoop fs -rm -r -f /ccs/output 
