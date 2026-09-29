```java
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ZoneConsumptionMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    private Map<String, String> zones = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {

        URI[] files = context.getCacheFiles();

        for (URI file : files) {

            File localFile = new File(
                    new File(file.getPath()).getName()
            );

            BufferedReader br = new BufferedReader(
                    new FileReader(localFile)
            );

            String line;

            while ((line = br.readLine()) != null) {

                String[] p = line.split(",");

                // Skip zones.csv header
                if (p[0].equalsIgnoreCase("zoneCode"))
                    continue;

                String zoneCode = p[0].trim();
                String zoneName = p[1].trim();
                String sanctionedLoad = p[2].trim();

                zones.put(
                        zoneCode,
                        zoneName + "," + sanctionedLoad
                );
            }

            br.close();
        }
    }

    @Override
    protected void map(
            LongWritable key,
            Text value,
            Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        String[] p = line.split(",");

        // readings.csv:
        // meterId,zoneCode,timestamp,consumption

        // Skip header
        if (p[0].equalsIgnoreCase("meterId"))
            return;

        String zoneCode = p[1].trim();

        // IMPORTANT:
        // p[2] = timestamp
        // p[3] = consumption
        String consumption = p[3].trim();

        if (zones.containsKey(zoneCode)) {

            String[] zoneInfo =
                    zones.get(zoneCode).split(",");

            String zoneName = zoneInfo[0];
            String sanctionedLoad = zoneInfo[1];

            context.write(
                    new Text(zoneCode),
                    new Text(
                            zoneName + "," +
                            consumption + "," +
                            sanctionedLoad
                    )
            );
        }
    }
}
```
