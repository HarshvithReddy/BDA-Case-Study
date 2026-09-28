# Problem 5 – Day-wise Consumption Trend for Top 3 Zones

## Objective

Identify the top three highest-consuming zones and determine their day-wise energy consumption trend.

## Part 1 – Top 3 Highest-Consuming Zones

### Objective

Identify the three zones with the highest total energy consumption over the given period.

### Approach

The Mapper extracts the `zoneCode` and `unitsConsumed` from each meter reading. The Reducer groups the readings by zone and calculates the total consumption for each zone.

The zone-wise totals are then used to identify the top three highest-consuming zones.

### Result

The top three highest-consuming zones identified are:

- **Z05 – 3310 units**
- **Z04 – 3100 units**
- **Z02 – 2850 units**

## Part 2 – Day-wise Consumption Trend

### Objective

Calculate the day-wise energy consumption trend for the top three highest-consuming zones.

### Approach

The Mapper filters the meter readings belonging to the top three zones: Z05, Z04, and Z02. It extracts the date from each timestamp and emits the zone, date, and corresponding `unitsConsumed`.

The Reducer groups the readings by zone and date and calculates the total consumption for each day.

## MapReduce Flow

### Part 1

```text
readings.csv
      ↓
     HDFS
      ↓
    Mapper
      ↓
 Shuffle & Sort
      ↓
   Reducer
      ↓
Total Consumption
    per Zone
      ↓
 Top 3 Zones
