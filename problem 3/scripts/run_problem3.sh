#!/bin/bash
set -e

echo "=========================================================================="
echo "  TEAM 8: VoltGrid Utilities - Smart Grid Energy Analytics"
echo "  Problem 3: Average Consumption Per Meter Within Each Zone"
echo "=========================================================================="

APP_DIR="/app"
SRC_DIR="${APP_DIR}/src/org/cbit/bda"
CLASSES_DIR="${APP_DIR}/classes"
JAR_FILE="${APP_DIR}/smartgrid-analytics.jar"
HDFS_INPUT="/input"
HDFS_OUTPUT="/output/problem3"

echo "[1/5] Compiling Java MapReduce source files..."
mkdir -p ${CLASSES_DIR}
rm -rf ${CLASSES_DIR}/*
javac -classpath $(hadoop classpath) -d ${CLASSES_DIR} ${SRC_DIR}/*.java
echo "Compilation successful."

echo "[2/5] Packaging classes into JAR: ${JAR_FILE}..."
jar -cvf ${JAR_FILE} -C ${CLASSES_DIR}/ .
echo "JAR created successfully."

echo "[3/5] Uploading datasets to HDFS..."
hdfs dfs -mkdir -p ${HDFS_INPUT}
hdfs dfs -put -f ${APP_DIR}/readings.csv ${HDFS_INPUT}/readings.csv
hdfs dfs -put -f ${APP_DIR}/zones.csv ${HDFS_INPUT}/zones.csv
echo "Files in HDFS ${HDFS_INPUT}:"
hdfs dfs -ls ${HDFS_INPUT}

echo "[4/5] Executing MapReduce Job on YARN with Map-Side Join..."
hdfs dfs -rm -r -f ${HDFS_OUTPUT} || true

hadoop jar ${JAR_FILE} org.cbit.bda.AvgConsumptionPerMeter \
    ${HDFS_INPUT}/readings.csv \
    ${HDFS_OUTPUT} \
    ${HDFS_INPUT}/zones.csv

echo "[5/5] Fetching and displaying MapReduce results from HDFS..."
echo "--------------------------------------------------------------------------"
echo "OUTPUT FOR PROBLEM 3 (Average Consumption per Meter within Each Zone):"
echo "--------------------------------------------------------------------------"
hdfs dfs -cat ${HDFS_OUTPUT}/part-r-00000
echo "--------------------------------------------------------------------------"

mkdir -p ${APP_DIR}/output
hdfs dfs -get -f ${HDFS_OUTPUT}/part-r-00000 ${APP_DIR}/output/part-r-00000
echo "Results saved locally to ${APP_DIR}/output/part-r-00000"
echo "=========================================================================="
