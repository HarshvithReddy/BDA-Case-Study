@echo off
REM ==============================================================================
REM   TEAM 8: VoltGrid Utilities - Smart Grid Energy Analytics
REM   Problem 3: Average Consumption Per Meter Within Each Zone
REM   Map-side Join via Distributed Cache on YARN
REM ==============================================================================

echo [INFO] Checking Docker containers...
docker ps --filter "name=resourcemanager" --format "{{.Names}}: {{.Status}}"

echo.
echo [1/4] Copying project files into Hadoop ResourceManager container...
docker exec resourcemanager mkdir -p /app
docker cp "%~dp0..\." resourcemanager:/app/

echo.
echo [2/4] Executing MapReduce Pipeline (Compile -> Ingest HDFS -> YARN Execution)...
docker exec resourcemanager bash -c "tr -d '\r' < /app/scripts/run_problem3.sh > /app/run.sh && chmod +x /app/run.sh && /app/run.sh"

echo.
echo [3/4] Copying results back to local BDA Folder...
if not exist "%~dp0..\output" mkdir "%~dp0..\output"
if not exist "%~dp0..\target" mkdir "%~dp0..\target"
docker cp resourcemanager:/app/output/part-r-00000 "%~dp0..\output\part-r-00000"
docker cp resourcemanager:/app/smartgrid-analytics.jar "%~dp0..\target\smartgrid-analytics.jar"

echo.
echo [4/4] Output from HDFS:
type "%~dp0..\output\part-r-00000"

echo.
echo [SUCCESS] Pipeline completed successfully!
pause
