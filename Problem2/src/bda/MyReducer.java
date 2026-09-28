import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class ZoneConsumptionReducer
        extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,
                           Context context)
            throws IOException, InterruptedException {

        double totalConsumption = 0;
        double sanctionedLoad = 0;
        String zoneName = "";

        for (Text value : values) {

            String[] parts = value.toString().split(",");

            zoneName = parts[0];

            double consumption =
                    Double.parseDouble(parts[1]);

            sanctionedLoad =
                    Double.parseDouble(parts[2]);

            totalConsumption += consumption;
        }

        double difference =
                totalConsumption - sanctionedLoad;

        String status;

        if (difference > 0)
            status = "EXCEEDING";
        else
            status = "WITHIN LIMIT";

        String output =
                "Total Consumption: " + totalConsumption +
                " | Sanctioned Load: " + sanctionedLoad +
                " | Difference: " + difference +
                " | " + status;

        context.write(
            new Text(zoneName),
            new Text(output)
        );
    }
}
