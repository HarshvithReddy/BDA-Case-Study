package org.cbit.bda;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * SmartGridDriver: Entry point to configure and launch the MapReduce job.
 */
public class SmartGridDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        if (args.length < 2) {
            System.err.println("Usage: SmartGridDriver <readings_input_path> <output_path> [zones_csv_path]");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        String zoneFile = (args.length >= 3) ? args[2] : "zones.csv";

        Job job = Job.getInstance(conf, "VoltGrid - Problem 3: Avg Consumption Per Meter Per Zone");
        job.setJarByClass(SmartGridDriver.class);

        job.setMapperClass(ZoneMapper.class);
        job.setReducerClass(ZoneReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Configure Distributed Cache for Map-side Join
        Path zoneHdfsPath = new Path(zoneFile);
        job.addCacheFile(zoneHdfsPath.toUri());
        job.getConfiguration().set("zone.file.path", zoneFile);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        Path out = new Path(outputPath);
        FileOutputFormat.setOutputPath(job, out);

        FileSystem fs = out.getFileSystem(conf);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
