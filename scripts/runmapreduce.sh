#!/bin/sh

cd /home/hadoop/fastq/

hdfs dfs -rm -r /user/hadoop/input/* /user/hadoop/output/* /user/hadoop/outputSort/*
hdfs dfs -mkdir -p /user/hadoop/input /user/hadoop/output /user/hadoop/outputSort
hdfs dfs -copyFromLocal /home/hadoop/fastq/input/fastq.txt /user/hadoop/input

hadoop jar /home/nemanja/fastq/out/fastq.jar impl.FastqCountSubsequences input/ output/ outputSort/ 15 2>joboutput
rm -rf output/part-r-00000 output/_SUCCESS
hdfs dfs -copyToLocal /user/hadoop/outputSort /home/hadoop/fastq/