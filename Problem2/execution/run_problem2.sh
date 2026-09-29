#!/bin/bash

echo "======================================"
echo " Problem 2 - Zone Consumption"
echo "======================================"

echo "[1] Removing old output..."

hdfs dfs -rm -r -f /output/problem2

echo "[2] Compiling Java programs..."

javac -classpath "$(hadoop classpath)" \
    ZoneConsumptionMapper.java \
    ZoneConsumptionReducer.java \
    ZoneConsumptionDriver.java

if [ $? -ne 0 ]; then
    echo "Compilation failed!"
    exit 1
fi

echo "[3] Creating JAR..."

jar -cvf problem2.jar \
    ZoneConsumptionMapper*.class \
    ZoneConsumptionReducer*.class \
    ZoneConsumptionDriver*.class

echo "[4] Running Hadoop MapReduce..."

hadoop jar problem2.jar \
    ZoneConsumptionDriver \
    /input/voltgrid/readings.csv \
    /output/problem2 \
    /input/voltgrid/zones.csv

if [ $? -ne 0 ]; then
    echo "MapReduce job failed!"
    exit 1
fi

echo "======================================"
echo " FINAL OUTPUT"
echo "======================================"

hdfs dfs -cat /output/problem2/part-r-00000

echo "======================================"
echo " Problem 2 Completed"
echo "======================================"
