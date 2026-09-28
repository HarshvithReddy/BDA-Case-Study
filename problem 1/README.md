# Problem 1 – Total Energy Consumption per Zone

## Objective

Calculate the total energy consumption for each zone using the electricity meter readings provided in the dataset.

## Dataset

The problem uses two input files:

- `readings.csv` – Contains meter readings with `meterId`, `zoneCode`, `timestamp`, and `unitsConsumed`.
- `zones.csv` – Contains zone information with `zoneCode`, `zoneName`, and `sanctionedLoadMW`.

Both datasets are stored in HDFS.

## Approach

The Hadoop MapReduce program processes the meter reading data using a Map-side Join.

The Mapper reads each meter reading and uses the `zoneCode` to obtain the corresponding zone name from the `zones.csv` dataset. It then emits the zone name along with its `unitsConsumed` value.

The Reducer groups the records by zone name and sums the consumption values to calculate the total energy consumption for each zone.

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
Total Consumption
     per Zone

