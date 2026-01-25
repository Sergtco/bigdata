#!/bin/bash

hdfs dfs -mkdir -p /user/hits/input
hdfs dfs -put /opt/hadoop/app/input.txt /user/hits/input/

hdfs dfs -cp /user/hits/input/input.txt /user/hits/data.txt

for i in {1..5}; do
  hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.2.jar \
    -files /opt/hadoop/app/mapper_auth.py,/opt/hadoop/app/reducer_auth.py \
    -mapper "python3 mapper_auth.py" \
    -reducer "python3 reducer_auth.py" \
    -input /user/hits/data.txt \
    -output /user/hits/output_auth_$i

  hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.2.jar \
    -files /opt/hadoop/app/mapper_hub.py,/opt/hadoop/app/reducer_hub.py \
    -mapper "python3 mapper_hub.py" \
    -reducer "python3 reducer_hub.py" \
    -input /user/hits/output_auth_$i/part-00000 \
    -output /user/hits/output_hub_$i

  hdfs dfs -rm /user/hits/data.txt
  hdfs dfs -cp /user/hits/output_hub_$i/part-00000 /user/hits/data.txt
done
