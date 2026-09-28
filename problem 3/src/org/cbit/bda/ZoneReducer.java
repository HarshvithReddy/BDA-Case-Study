package org.cbit.bda;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * ZoneReducer: Computes the average consumption per meter within each zone.
 */
public class ZoneReducer extends Reducer<Text, Text, Text, Text> {

    private final Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Double> meterTotalMap = new HashMap<>();
        double totalZoneUnits = 0.0;
        int totalReadings = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(":");
            if (parts.length == 2) {
                String meterId = parts[0].trim();
                try {
                    double units = Double.parseDouble(parts[1].trim());
                    meterTotalMap.put(meterId, meterTotalMap.getOrDefault(meterId, 0.0) + units);
                    totalZoneUnits += units;
                    totalReadings++;
                } catch (NumberFormatException ignored) {}
            }
        }

        int distinctMeters = meterTotalMap.size();
        double avgConsumptionPerMeter = distinctMeters > 0 ? (totalZoneUnits / distinctMeters) : 0.0;

        StringBuilder breakdown = new StringBuilder("[");
        boolean first = true;
        for (Map.Entry<String, Double> entry : meterTotalMap.entrySet()) {
            if (!first) breakdown.append(", ");
            breakdown.append(entry.getKey()).append(": ").append(String.format("%.1f", entry.getValue())).append("u");
            first = false;
        }
        breakdown.append("]");

        String outputStr = String.format("Avg Per Meter: %8.2f units | Total Zone Consumption: %7.2f units | Unique Meters: %2d | Readings: %2d | Meters: %s",
                avgConsumptionPerMeter,
                totalZoneUnits,
                distinctMeters,
                totalReadings,
                breakdown.toString());

        outValue.set(outputStr);
        context.write(key, outValue);
    }
}
