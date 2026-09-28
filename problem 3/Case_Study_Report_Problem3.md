# CHAITANYA BHARATHI INSTITUTE OF TECHNOLOGY (A)
### DEPARTMENT OF COMPUTER SCIENCE AND ENGINEERING
**Course:** BIG DATA ANALYTICS (22CSE19 PE-I)  
**Semester:** VI Semester / Academic Year 2026-2027  
**Assignment:** End-to-End Big Data Analytics Case Study Project  

---

# CASE STUDY REPORT: PROBLEM 3
## Smart Grid Energy Analytics — Average Consumption Per Meter Within Each Zone
### Team 8: VoltGrid Utilities
**Assigned Focus Technique:** Map-side Join using Distributed Cache  

**Submitted By:**  
- **Name:** CILVARI HARSHVITH REDDY  
- **Hall Ticket No:** 160124733317  
- **Team:** Team 8  
- **Assigned Problem:** Problem 3  

---

## 1. INTRODUCTION & PROBLEM FORMULATION

### 1.1 Organizational Background
VoltGrid Utilities manages a multi-zone regional power distribution grid. Smart meters installed across consumer nodes report continuous periodic load and power consumption logs to central operational monitoring clusters. 

### 1.2 Problem Statement 3
> **"What is the average consumption per meter within each zone?"**

To assess regional energy usage patterns, load density, and infrastructure requirements, VoltGrid needs to compute the **average energy consumed per smart meter** across each operational zone.

### 1.3 Key Objectives
1. Perform an efficient **Map-Side Join** between the high-volume transactional smart meter stream (`readings.csv`) and the static reference lookup table (`zones.csv`) using Hadoop's **Distributed Cache**.
2. Group readings by zone without requiring a network-intensive Reduce-side shuffle for the dimension table.
3. Calculate:
   - Total energy units consumed across each zone.
   - Count of unique/distinct smart meters operating in each zone.
   - The quotient: **$\text{Average Consumption Per Meter} = \frac{\text{Total Zone Consumption}}{\text{Distinct Meters Count}}$**.
4. Output detailed per-meter consumption metrics alongside the zone-level averages for comprehensive analysis.

---

## 2. DATASETS SPECIFICATION

The case study leverages two distinct datasets:

### 2.1 Zone Master Table (`zones.csv`) — Small Dimension Table
- **Nature**: Tiny, rarely-changing reference table (5 records, 131 bytes).
- **Distribution Mechanism**: Broadcasted to all worker nodes via Hadoop Distributed Cache.
- **Schema**:
  | Column Name | Data Type | Description | Sample Value |
  |:---|:---|:---|:---|
  | `zoneCode` | String | Unique zone identifier | `Z01` |
  | `zoneName` | String | Geographic zone label | `North Zone` |
  | `sanctionedLoadMW` | Double | Maximum allowable power draw in MW | `500` |

### 2.2 Meter Readings Log (`readings.csv`) — Fact Table / Streaming Logs
- **Nature**: High-volume, continuous timeseries log of energy consumption.
- **Schema**:
  | Column Name | Data Type | Description | Sample Value |
  |:---|:---|:---|:---|
  | `meterId` | String | Smart meter serial ID | `M001` |
  | `zoneCode` | String | Foreign key reference to zone | `Z01` |
  | `timestamp` | Timestamp | Timestamp of reading (YYYY-MM-DD HH:MM) | `2026-09-01 08:00` |
  | `unitsConsumed` | Double | Electrical units consumed (kWh) | `120` |

---

## 3. ARCHITECTURE & MAP-SIDE JOIN TECHNIQUE

### 3.1 Why Map-Side Join over Reduce-Side Join?
In traditional MapReduce data pipelines:
- **Reduce-Side Join**: Both large and small datasets pass through the Mapper, are tagged with their origin table, and are pushed through the **Shuffle and Sort** phase over the cluster network. All records for the same key travel across the network before joining inside the Reducer.
  - *Disadvantage*: High network bandwidth consumption, disk I/O spilling, and high latency.
- **Map-Side Join using Distributed Cache**:
  - The small lookup table (`zones.csv`) is copied to each NodeManager's local disk once before tasks execute via Hadoop's Distributed Cache.
  - During the Mapper's initialization phase (`setup()`), the file is read from local disk into an in-memory hash table (`HashMap<String, String>`).
  - As each reading record arrives in `map()`, its `zoneCode` is looked up in $O(1)$ constant time and resolved to `zoneName`.
  - The joined key (`zoneName`) is directly emitted.
  - *Advantage*: Eliminates the network shuffle of the lookup table entirely, saving massive bandwidth and reducing job execution latency.

### 3.2 End-to-End Pipeline Architecture
```mermaid
flowchart TD
    subgraph Storage Ingestion (HDFS)
        Z[zones.csv in HDFS: /input/zones.csv]
        R[readings.csv in HDFS: /input/readings.csv]
    end

    subgraph Distributed Cache Localisation
        Z -->|job.addCacheFile| DC[NodeManager Local Cache Directory]
    end

    subgraph Mapper Phase: Map-Side Join
        DC -->|setup: parse lines| HT[In-Memory HashMap: zoneCode to zoneName]
        R -->|Line RecordReader| MAP[AvgConsumptionMapper.map]
        HT -->|O1 Hash Lookup| MAP
        MAP -->|Emit: Key=zoneName, Value=meterId:units| PART[Partitioner & Shuffle]
    end

    subgraph Reducer Phase: Aggregation
        PART --> RED[AvgConsumptionReducer.reduce]
        RED -->|Aggregate: Total Units & Unique Meters| CALC[Calculate: TotalUnits / UniqueMeters]
        CALC --> OUT[HDFS: /output/problem3/part-r-00000]
    end
```

---

## 4. MAPREDUCE ALGORITHM & SOURCE CODE IMPLEMENTATION

### 4.1 Mapper Algorithm (`AvgConsumptionMapper`)
1. **`setup(Context context)`**:
   - Access cached files via `context.getCacheFiles()`.
   - Read lines from `zones.csv`, split by comma `,`.
   - Store key-value pairs `(zoneCode, zoneName)` into `zoneNameMap`.
2. **`map(LongWritable key, Text value, Context context)`**:
   - Read row from `readings.csv`.
   - If row is header (`meterId,...`), skip.
   - Parse `meterId = tokens[0]`, `zoneCode = tokens[1]`, `units = Double.parseDouble(tokens[3])`.
   - Resolve `zoneName = zoneNameMap.getOrDefault(zoneCode, zoneCode)`.
   - Emit `(Text(zoneName), Text(meterId + ":" + units))`.

### 4.2 Reducer Algorithm (`AvgConsumptionReducer`)
1. **`reduce(Text key, Iterable<Text> values, Context context)`**:
   - Initialize:
     - `meterTotalMap = HashMap<String, Double>()`
     - `totalZoneUnits = 0.0`
     - `totalReadings = 0`
   - For each `value` in `values`:
     - Split into `meterId` and `units`.
     - `meterTotalMap[meterId] += units`
     - `totalZoneUnits += units`
     - `totalReadings++`
   - Calculate distinct meters: `distinctMeters = meterTotalMap.size()`.
   - Calculate average:
     $$\text{avgConsumptionPerMeter} = \frac{\text{totalZoneUnits}}{\text{distinctMeters}}$$
   - Emit `(key, FormattedSummaryString)`.

### 4.3 Driver Configuration (`AvgConsumptionPerMeter.java`)
- Sets Job Name: `"VoltGrid - Problem 3: Avg Consumption Per Meter Per Zone"`.
- Configures Distributed Cache: `job.addCacheFile(new Path(zoneFile).toUri())`.
- Sets Input Format, Output Format, Mapper, and Reducer.
- Removes existing output directory to guarantee deterministic rerun.

---

## 5. EXPERIMENTAL EXECUTION & RESULTS

### 5.1 Step-by-Step Execution Log
The MapReduce job was compiled, ingested, and run on a multi-node Dockerized Hadoop cluster on YARN:
```bash
# 1. Compile Java files with Hadoop Classpath
javac -classpath $(hadoop classpath) -d /app/classes /app/src/org/cbit/bda/*.java

# 2. Package into JAR
jar -cvf /app/smartgrid-analytics.jar -C /app/classes/ .

# 3. Ingest Datasets into HDFS
hdfs dfs -mkdir -p /input
hdfs dfs -put -f /app/readings.csv /input/readings.csv
hdfs dfs -put -f /app/zones.csv /input/zones.csv

# 4. Submit Job to YARN
hadoop jar /app/smartgrid-analytics.jar org.cbit.bda.AvgConsumptionPerMeter \
    /input/readings.csv \
    /output/problem3 \
    /input/zones.csv
```

### 5.2 YARN ResourceManager Execution Metrics
From the YARN ResourceManager web interface (`http://localhost:8088/cluster/apps`):
- **Application ID:** `application_1790508421544_0002`
- **Application Type:** `MAPREDUCE`
- **State:** `FINISHED`
- **Final Status:** `SUCCEEDED`
- **Map Tasks:** 1 (100% completed)
- **Reduce Tasks:** 1 (100% completed)
- **Map Input Records:** 46
- **Map Output Records:** 45 (1 header skipped)
- **Reduce Input Groups:** 5 (5 distinct zones)
- **Reduce Output Records:** 5 (one summary line per zone)

### 5.3 Output Verification & Analysis Table

#### Raw MapReduce Output (`/output/problem3/part-r-00000`):
```text
Central Zone	Avg Per Meter:  1103.33 units | Total Zone Consumption: 3310.00 units | Unique Meters:  3 | Readings:  9 | Meters: [M015: 1420.0u, M014: 1110.0u, M013: 780.0u]
East Zone	Avg Per Meter:   766.67 units | Total Zone Consumption: 2300.00 units | Unique Meters:  3 | Readings:  9 | Meters: [M008: 690.0u, M007: 480.0u, M009: 1130.0u]
North Zone	Avg Per Meter:   586.67 units | Total Zone Consumption: 1760.00 units | Unique Meters:  3 | Readings:  9 | Meters: [M003: 800.0u, M002: 570.0u, M001: 390.0u]
South Zone	Avg Per Meter:   950.00 units | Total Zone Consumption: 2850.00 units | Unique Meters:  3 | Readings:  9 | Meters: [M004: 630.0u, M006: 1350.0u, M005: 870.0u]
West Zone	Avg Per Meter:  1033.33 units | Total Zone Consumption: 3100.00 units | Unique Meters:  3 | Readings:  9 | Meters: [M011: 960.0u, M010: 570.0u, M012: 1570.0u]
```

#### Analytical Breakdown:
| Zone Name | Zone Code | Sanctioned Load | Total Zone Units | Unique Meters | Avg Consumption / Meter | Consumption Density Rank |
|:---|:---:|:---:|:---:|:---:|:---:|:---:|
| **Central Zone** | Z05 | 1000 MW | 3,310.00 units | 3 meters | **1,103.33 units/meter** | 1 (Highest) |
| **West Zone** | Z04 | 800 MW | 3,100.00 units | 3 meters | **1,033.33 units/meter** | 2 |
| **South Zone** | Z02 | 700 MW | 2,850.00 units | 3 meters | **950.00 units/meter** | 3 |
| **East Zone** | Z03 | 600 MW | 2,300.00 units | 3 meters | **766.67 units/meter** | 4 |
| **North Zone** | Z01 | 500 MW | 1,760.00 units | 3 meters | **586.67 units/meter** | 5 (Lowest) |

### 5.4 Business Insights
1. **Highest Per-Meter Demand**: **Central Zone (Z05)** has the highest average consumption per meter (1,103.33 units), with individual meter M015 contributing 1,420 units. This indicates high industrial or commercial concentration.
2. **Lowest Per-Meter Demand**: **North Zone (Z01)** exhibits the lowest average consumption per meter (586.67 units), suggesting residential or low-density load profiles.
3. **Correlation with Sanctioned Load**: The average consumption per meter strictly follows the ordering of the sanctioned load capacity (Central > West > South > East > North), validating that VoltGrid's allocated infrastructure is appropriately provisioned for current demand levels.

---

## 6. VIVA VOCE & TECHNICAL INTERVIEW PREPARATION

### Q1: What is the main advantage of a Map-side Join over a Reduce-side Join in Hadoop?
**Answer:** In a Reduce-side join, all records from both datasets must travel through the network shuffle phase to be joined at the reducer. This causes massive network congestion and disk spilling when joining large fact tables with reference datasets. A Map-side join avoids the shuffle phase entirely by distributing the small table via Hadoop's Distributed Cache to every worker node. The join is completed directly inside the `map()` method in memory with $O(1)$ lookup time.

### Q2: When is a Map-side Join NOT suitable?
**Answer:** A Map-side join is only suitable when at least one of the datasets is small enough to fit comfortably into the JVM heap memory of each Mapper task without causing `OutOfMemoryError` (OOM). If both datasets are massive (e.g. hundreds of gigabytes), a Reduce-side join, Sort-Merge-Bucket (SMB) join, or bucketing strategy is required.

### Q3: How does Hadoop's Distributed Cache distribute files to worker nodes?
**Answer:** The file is registered via `job.addCacheFile(URI)`. Before the application master launches tasks on worker nodes (NodeManagers), YARN downloads the cached file from HDFS and stores it in a localized private directory on the local filesystem of each worker node. When the container executes, it creates a local symlink in the task's working directory, allowing mappers to open and read it as a standard local file.

### Q4: Why can't we compute the distinct meter count in a MapReduce Combiner?
**Answer:** A Combiner is a mini-reducer that runs locally on mapper output before the shuffle. While sum and count of additive metrics can be combined, counting distinct items across multiple mapper splits requires seeing the complete set of keys across all splits. If two mappers observe the same `meterId`, combining locally would count duplicates or lose the global cardinality. Therefore, distinct meter aggregation must be performed at the Reducer.

### Q5: What is the time and space complexity of your implementation?
**Answer:**
- **Time Complexity:** $O(N)$ where $N$ is the number of meter readings. Each reading is processed once with an $O(1)$ HashMap lookup in the mapper and an $O(1)$ HashMap accumulation in the reducer.
- **Space Complexity:** $O(Z + M_z)$ where $Z$ is the number of zones (stored in the mapper's memory from Distributed Cache) and $M_z$ is the number of distinct meters in a single zone (stored in the reducer's memory). Since $Z=5$ and $M_z=3$, memory footprint is minimal and scales efficiently.

---

## 7. CONCLUSION
The MapReduce application successfully implemented the **Map-Side Join using Distributed Cache** on Hadoop YARN to solve Problem 3 for Team 8 (VoltGrid Utilities). The solution accurately determined that the **Central Zone** experiences the highest average power consumption per meter (1,103.33 units/meter), while the **North Zone** experiences the lowest (586.67 units/meter). All results have been validated against mathematical ground truth and verified on the YARN ResourceManager UI.
