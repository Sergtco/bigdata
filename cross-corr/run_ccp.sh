#!/bin/bash
./hdpCompile.sh ./CrossCorrelationPairs.java
hadoop fs -mkdir -p /ccp/input
hadoop fs -put -p ./order_db.txt /ccp/input/order_db.txt
hadoop jar CrossCorrelationPairs.jar CrossCorrelationPairs /ccp/input /ccp/output 
hadoop fs -copyToLocal /ccp/output ./ccp-output
hadoop fs -rm -r -f /ccp/output 
