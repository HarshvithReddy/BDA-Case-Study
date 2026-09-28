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
import org.apache.hadoop.mapreduce.Mapper;

/**
 * ZoneMapper: Implements Map-side join for VoltGrid Readings.
 * Emits (ZoneName, meterId:unitsConsumed)
 */
public class ZoneMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Map<String, String> zoneNameMap = new HashMap<>();
    private final Map<String, Double> zoneLoadMap = new HashMap<>();

    private final Text outKey = new Text();
    private final Text outValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        boolean loaded = false;

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

        if (!loaded) {
            File directFile = new File("zones.csv");
            if (directFile.exists()) {
                loadZoneDataFromFile(directFile);
                loaded = true;
            }
        }

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
            System.err.println("WARNING: zones.csv could not be loaded from Distributed Cache or filesystem.");
        } else {
            System.out.println("SUCCESS: Loaded " + zoneNameMap.size() + " zones into in-memory HashMap.");
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
                continue;
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
            return;
        }

        String[] tokens = line.split(",");
        if (tokens.length >= 4) {
            String meterId = tokens[0].trim();
            String zoneCode = tokens[1].trim();
            String unitsStr = tokens[3].trim();

            try {
                double units = Double.parseDouble(unitsStr);
                String zoneName = zoneNameMap.getOrDefault(zoneCode, zoneCode);

                outKey.set(zoneName);
                outValue.set(meterId + ":" + units);
                context.write(outKey, outValue);
            } catch (NumberFormatException ignored) {}
        }
    }
}
