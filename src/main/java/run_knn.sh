#!/bin/bash

export BOOK_HOME=/Users/admin/code/Iron_Man
export SPARK_MASTER=spark://localhost:7077

k=4
d=2
APP_JAR=$BOOK_HOME/target/iron_man.jar

r="r.txt"
s="s.txt"
OTHER_JARS=""

prog=SparkKNN

$SPARK_HOME/bin/spark-submit  \
    --master yarn \
    --num-executors 2 \
    --driver-memory 1g \
    --executor-memory 2g \
    --executor-cores 2 \
    --class $prog \
    $APP_JAR $k $d $r $s
