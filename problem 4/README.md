# Problem 4 – Peak-Hour Consumption

## Objective

Identify the zone with the highest energy consumption during peak hours.

## Assumption

Since the given dataset does not specify a fixed peak-hour period, we assume **18:00 to 21:00** as the peak-hour period for this analysis.

## Dataset

The problem uses two input files:

- `readings.csv` – Contains meter readings with `meterId`, `zoneCode`, `timestamp`, and `unitsConsumed`.
- `zones.csv` – Contains zone information with `zoneCode`, `zoneName`, and `sanctionedLoadMW`.

Both datasets are stored in HDFS.

## Approach

The Hadoop MapReduce program processes the meter reading data to identify consumption during the defined peak-hour period.

The Mapper extracts the zone code, timestamp, and `unitsConsumed` from each meter reading and filters the records that fall between 18:00 and 21:00. It then emits the zone along with the corresponding consumption value.

The Reducer groups the peak-hour readings by zone and compares the consumption values to identify the zone with the highest peak-hour consumption.

## MapReduce Flow

```text
readings.csv + zones.csv
          ↓
         HDFS
          ↓
        Mapper
          ↓
  Peak-Hour Filtering
          ↓
    Shuffle & Sort
          ↓
       Reducer
          ↓
Highest Peak-Hour
Consumption Zone
