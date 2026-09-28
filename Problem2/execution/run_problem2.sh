#!/bin/bash
set -e

echo "=============================================================="
echo "TEAM 8: VoltGrid Utilities"
echo "Problem 2: Zone Operating Load vs Sanctioned Load"
echo "=============================================================="

echo "[1/5] Compiling Java files..."
rm -f *.class
javac -cp "$(hadoop classpath)" MyMapper.java MyReducer.java Driver.java
echo "Compilation successful."

echo "[2/5] Creating JAR..."
jar -cvf zone-load.jar MyMapper.class MyReducer.class Driver.class
echo "JAR created successfully."

echo "[3/5] Checking HDFS input..."
hdfs dfs -ls /input/voltgrid

echo "[4/5] Running MapReduce job..."
hdfs dfs -rm -r -f /output/problem2 || true
hadoop jar zone-load.jar Driver

echo "[5/5] Showing result..."
echo "--------------------------------------------------------------"
hdfs dfs -cat /output/problem2/part-r-00000
echo "--------------------------------------------------------------"

echo "Job completed successfully."
