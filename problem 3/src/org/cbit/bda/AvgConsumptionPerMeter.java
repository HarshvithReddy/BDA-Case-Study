package org.cbit.bda;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * BDA Case Study - Team 8: VoltGrid Utilities (Smart Grid Energy Analytics)
 * Problem 3: What is the average consumption per meter within each zone?
 * 
 * Recommended Technique: Map-Side Join using Distributed Cache
 * - Mapper loads zones.csv from Distributed Cache into an in-memory HashMap in setup()
 * - Maps zoneCode -> zoneName directly inside map() without reduce-side shuffle
 * - Reducer calculates total units and counts distinct meters per zone to compute:
 *   Average Consumption Per Meter = (Total Zone Units) / (Distinct Meters Count)
 */
public class AvgConsumptionPerMeter {

    /**
     * Mapper Class: Performs Map-Side Join using Distributed Cache.
     * Input Key: Byte offset (LongWritable)
     * Input Value: Line from readings.csv (Text)
     * Output Key: Zone Name (Text)
     * Output Value: meterId:unitsConsumed (Text)
     */
    public static class AvgConsumptionMapper extends Mapper<LongWritable, Text, Text, Text> {

        // In-memory lookup tables populated from Distributed Cache
        private final Map<String, String> zoneNameMap = new HashMap<>();
        private final Map<String, Double> zoneLoadMap = new HashMap<>();

        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            boolean loaded = false;

            // 1. Attempt to load from Hadoop Distributed Cache URIs
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheUri : cacheFiles) {
                    Path path = new Path(cacheUri.getPath());
                    String fileName = path.getName();
                    if (fileName.contains("zones.csv")) {
                        File localFile = new File(fileName);
                        if (localFile.exists()) {
                            loadZoneDataFromFile(localFile);
                            loaded = true;
                            break;
                        }
                    }
                }
            }

            // 2. Fallback: Check local working directory for symlinked zones.csv
            if (!loaded) {
                File directFile = new File("zones.csv");
                if (directFile.exists()) {
                    loadZoneDataFromFile(directFile);
                    loaded = true;
                }
            }

            // 3. Fallback: Check HDFS path passed via configuration property
            if (!loaded) {
                String confPath = conf.get("zone.file.path");
                if (confPath != null && !confPath.isEmpty()) {
                    Path hdfsPath = new Path(confPath);
                    FileSystem fs = FileSystem.get(conf);
                    if (fs.exists(hdfsPath)) {
                        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(hdfsPath)))) {
                            parseZoneStream(br);
                            loaded = true;
                        }
                    }
                }
            }

            if (!loaded) {
                System.err.println("WARNING: zones.csv could not be loaded from Distributed Cache or filesystem. Using fallback.");
            } else {
                System.out.println("SUCCESS: Loaded " + zoneNameMap.size() + " zones into in-memory HashMap for Map-Side Join.");
            }
        }

        private void loadZoneDataFromFile(File file) throws IOException {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                parseZoneStream(reader);
            }
        }

        private void parseZoneStream(BufferedReader reader) throws IOException {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("zoneCode")) {
                    continue; // Skip header line or empty rows
                }
                String[] parts = line.split(",");
                if (parts.length >= 2) {
                    String zoneCode = parts[0].trim();
                    String zoneName = parts[1].trim();
                    zoneNameMap.put(zoneCode, zoneName);

                    if (parts.length >= 3) {
                        try {
                            zoneLoadMap.put(zoneCode, Double.parseDouble(parts[2].trim()));
                        } catch (NumberFormatException ignored) {}
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("meterId")) {
                return; // Skip CSV header
            }

            // readings.csv schema: meterId, zoneCode, timestamp, unitsConsumed
            String[] tokens = line.split(",");
            if (tokens.length >= 4) {
                String meterId = tokens[0].trim();
                String zoneCode = tokens[1].trim();
                String timestamp = tokens[2].trim();
                String unitsStr = tokens[3].trim();

                try {
                    double units = Double.parseDouble(unitsStr);

                    // Perform Map-side join: resolve zoneCode to zoneName using HashMap
                    String zoneName = zoneNameMap.getOrDefault(zoneCode, zoneCode);

                    // Emit: Key = Zone Name, Value = meterId:unitsConsumed
                    outKey.set(zoneName);
                    outValue.set(meterId + ":" + units);
                    context.write(outKey, outValue);

                } catch (NumberFormatException e) {
                    System.err.println("Skipping malformed reading: " + line);
                }
            }
        }
    }

    /**
     * Reducer Class: Computes average consumption per meter within each zone.
     * Input Key: Zone Name (Text)
     * Input Values: Iterable of meterId:unitsConsumed (Text)
     * Output Key: Zone Name (Text)
     * Output Value: Formatted Average Consumption & Breakdown Statistics (Text)
     */
    public static class AvgConsumptionReducer extends Reducer<Text, Text, Text, Text> {

        private final Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Map to aggregate total consumption per meter within this zone
            Map<String, Double> meterTotalMap = new HashMap<>();
            Map<String, Integer> meterCountMap = new HashMap<>();
            double totalZoneUnits = 0.0;
            int totalReadings = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(":");
                if (parts.length == 2) {
                    String meterId = parts[0].trim();
                    try {
                        double units = Double.parseDouble(parts[1].trim());
                        meterTotalMap.put(meterId, meterTotalMap.getOrDefault(meterId, 0.0) + units);
                        meterCountMap.put(meterId, meterCountMap.getOrDefault(meterId, 0) + 1);
                        totalZoneUnits += units;
                        totalReadings++;
                    } catch (NumberFormatException ignored) {}
                }
            }

            int distinctMeters = meterTotalMap.size();
            double avgConsumptionPerMeter = distinctMeters > 0 ? (totalZoneUnits / distinctMeters) : 0.0;

            // Build detailed meter breakdown string for comprehensive analytics
            StringBuilder breakdown = new StringBuilder();
            breakdown.append("[");
            boolean first = true;
            for (Map.Entry<String, Double> entry : meterTotalMap.entrySet()) {
                if (!first) breakdown.append(", ");
                breakdown.append(entry.getKey())
                         .append(": ")
                         .append(String.format("%.1f", entry.getValue()))
                         .append("u");
                first = false;
            }
            breakdown.append("]");

            // Formatted Tabular Output:
            // Average Per Meter (units) | Total Zone Units | Distinct Meters | Total Readings | Meter Details
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

    /**
     * Driver Class: Configures and triggers the MapReduce job on YARN.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        if (args.length < 2) {
            System.err.println("Usage: AvgConsumptionPerMeter <readings_input_path> <output_path> [zones_csv_path]");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        String zoneFile = (args.length >= 3) ? args[2] : "zones.csv";

        Job job = Job.getInstance(conf, "VoltGrid - Problem 3: Avg Consumption Per Meter Per Zone");
        job.setJarByClass(AvgConsumptionPerMeter.class);

        job.setMapperClass(AvgConsumptionMapper.class);
        job.setReducerClass(AvgConsumptionReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Add zones.csv to Distributed Cache for Map-Side Join
        Path zoneHdfsPath = new Path(zoneFile);
        job.addCacheFile(zoneHdfsPath.toUri());
        job.getConfiguration().set("zone.file.path", zoneFile);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        Path out = new Path(outputPath);
        FileOutputFormat.setOutputPath(job, out);

        // Automatically clean output directory if it exists to allow immediate rerun
        FileSystem fs = out.getFileSystem(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
            System.out.println("Removed existing output directory: " + outputPath);
        }

        System.out.println("=================================================================");
        System.out.println("  TEAM 8: VoltGrid Utilities - Smart Grid Energy Analytics");
        System.out.println("  Problem 3: Average Consumption Per Meter Within Each Zone");
        System.out.println("  Map-Side Join File (Distributed Cache): " + zoneFile);
        System.out.println("  Readings Input: " + inputPath);
        System.out.println("  Output Destination: " + outputPath);
        System.out.println("=================================================================");

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
