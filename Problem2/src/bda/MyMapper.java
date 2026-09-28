import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class ZoneConsumptionMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    private Map<String, String> zoneMap = new HashMap<>();

    @Override
    protected void setup(Context context)
            throws IOException {

        URI[] files = context.getCacheFiles();

        for (URI file : files) {
            BufferedReader br = new BufferedReader(
                    new FileReader(new File(file.getPath()))
            );

            String line;

            while ((line = br.readLine()) != null) {

                String[] parts = line.split(",");

                // zones.csv:
                // zoneCode,zoneName,sanctionedLoadMW

                String zoneCode = parts[0].trim();
                String zoneName = parts[1].trim();
                String sanctionedLoad = parts[2].trim();

                zoneMap.put(
                    zoneCode,
                    zoneName + "," + sanctionedLoad
                );
            }

            br.close();
        }
    }

    @Override
    protected void map(LongWritable key, Text value,
                       Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip header
        if (line.toLowerCase().contains("zonecode"))
            return;

        String[] parts = line.split(",");

        // meter.csv:
        // meterId,zoneCode,unitsConsumed

        String zoneCode = parts[1].trim();
        String consumption = parts[2].trim();

        if (zoneMap.containsKey(zoneCode)) {

            String[] zoneInfo = zoneMap.get(zoneCode).split(",");

            String zoneName = zoneInfo[0];
            String sanctionedLoad = zoneInfo[1];

            // key = zoneCode
            // value = zoneName,consumption,sanctionedLoad

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
