```java
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ZoneConsumptionReducer
        extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,
                           Context context)
            throws IOException, InterruptedException {

        double totalConsumption = 0.0;
        double sanctionedLoad = 0.0;
        String zoneName = "";

        for (Text value : values) {

            String[] p = value.toString().split(",");

            zoneName = p[0];

            totalConsumption +=
                Double.parseDouble(p[1]);

            sanctionedLoad =
                Double.parseDouble(p[2]);
        }

        double difference =
            totalConsumption - sanctionedLoad;

        String status;

        if (difference > 0)
            status = "EXCEEDING";
        else
            status = "WITHIN LIMIT";

        String result =
            "Total Consumption: " + totalConsumption +
            " | Sanctioned Load: " + sanctionedLoad +
            " | Difference: " + difference +
            " | " + status;

        context.write(
            new Text(key.toString() + "    " + zoneName),
            new Text(result)
        );
    }
}
```
