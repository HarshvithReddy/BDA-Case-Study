
# Problem 4 – Identify Zones Exceeding Their Sanctioned Load

## Objective

Identify the zones whose total energy consumption exceeds their sanctioned load and calculate the difference between total consumption and sanctioned load.

## Dataset

The problem uses two input files:

- `readings.csv` – Contains meter readings with `meterId`, `zoneCode`, `timestamp`, and `unitsConsumed`.
- `zones.csv` – Contains zone information with `zoneCode`, `zoneName`, and `sanctionedLoadMW`.

Both datasets are stored in HDFS.

## Approach

The Hadoop MapReduce program processes the electricity meter readings and zone information using a **Map-side Join**.

The Mapper reads each meter reading and uses the `zoneCode` to obtain the corresponding zone name and sanctioned load from the `zones.csv` dataset. It emits the zone information along with the `unitsConsumed` value.

The Reducer groups all records belonging to the same zone and calculates the **total energy consumption** by summing the meter readings.

The Reducer then compares the total consumption with the sanctioned load and calculates the difference:

**Difference = Total Consumption − Sanctioned Load**

If the difference is greater than zero, the zone is classified as **EXCEEDING**.

## MapReduce Flow

```text
readings.csv + zones.csv
          ↓
         HDFS
          ↓
        Mapper
          ↓
    Map-side Join
          ↓
    Shuffle & Sort
          ↓
       Reducer
          ↓
 Calculate Total Consumption
          ↓
 Compare with Sanctioned Load
          ↓
 Calculate Difference
          ↓
 EXCEEDING / WITHIN LIMIT




Output
Z01    North Zone    Total Consumption: 1760.0 | Sanctioned Load: 500.0 | Difference: 1260.0 | EXCEEDING
Z02    South Zone    Total Consumption: 2850.0 | Sanctioned Load: 700.0 | Difference: 2150.0 | EXCEEDING
Z03    East Zone     Total Consumption: 2300.0 | Sanctioned Load: 600.0 | Difference: 1700.0 | EXCEEDING
Z04    West Zone     Total Consumption: 3100.0 | Sanctioned Load: 800.0 | Difference: 2300.0 | EXCEEDING
Z05    Central Zone  Total Consumption: 3310.0 | Sanctioned Load: 1000.0 | Difference: 2310.0 | EXEEDING
