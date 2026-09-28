import java.io.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "VoltGrid Problem 2");

        job.setJarByClass(Driver.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Give zones.csv to every Mapper
        job.addCacheFile(
            new URI("hdfs:///input/voltgrid/zones.csv#zones.csv")
        );

        // Read readings.csv
        FileInputFormat.addInputPath(
            job,
            new Path("/input/voltgrid/readings.csv")
        );

        // Output directory
        FileOutputFormat.setOutputPath(
            job,
            new Path("/output/problem2")
        );

        System.exit(
            job.waitForCompletion(true) ? 0 : 1
        );
    }
}
