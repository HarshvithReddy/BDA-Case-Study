import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ZoneConsumptionDriver {

    public static void main(String[] args)
            throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "Zone Consumption and Sanctioned Load"
        );

        job.setJarByClass(
                ZoneConsumptionDriver.class
        );

        // Mapper
        job.setMapperClass(
                ZoneConsumptionMapper.class
        );

        // Reducer
        job.setReducerClass(
                ZoneConsumptionReducer.class
        );

        // Output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input directory
        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );

        // Output directory
        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );

        // Give zones.csv to Mapper
        job.addCacheFile(
                new Path(args[2]).toUri()
        );

        System.exit(
                job.waitForCompletion(true)
                        ? 0 : 1
        );
    }
}
