#!/bin/sh

cd /home/hadoop/fastq/

hdfs dfs -rm /user/hadoop/input/* /user/hadoop/output/*
hdfs dfs -mkdir -p /user/hadoop/input /user/hadoop/output
hdfs dfs -copyFromLocal /home/hadoop/fastq/input/fastq.txt /user/hadoop/input

hadoop jar /home/nemanja/fastq/out/fastq.jar impl.FastqCountBaseDriver input/ output/

rm -rf output/*

hdfs dfs -copyToLocal /user/hadoop/output