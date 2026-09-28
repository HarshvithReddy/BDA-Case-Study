import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ZoneMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private HashMap<String, String> zoneMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {

        Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());

        for (Path file : files) {

            BufferedReader br = new BufferedReader(
                    new FileReader(file.toString()));

            String line;

            while ((line = br.readLine()) != null) {

                String[] parts = line.split(",");

                // Skip header
                if (parts[0].equals("zoneCode")) {
                    continue;
                }

                String zoneCode = parts[0].trim();
                String zoneName = parts[1].trim();

                zoneMap.put(zoneCode, zoneName);
            }

            br.close();
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip readings header
        if (line.startsWith("meterId")) {
            return;
        }

        String[] parts = line.split(",");

        if (parts.length != 4) {
            return;
        }

        String zoneCode = parts[1].trim();
        double unitsConsumed = Double.parseDouble(parts[3].trim());

        // Find zone name using zoneCode
        String zoneName = zoneMap.get(zoneCode);

        if (zoneName != null) {

            context.write(
                    new Text(zoneName),
                    new DoubleWritable(unitsConsumed)
            );
        }
    }
}
