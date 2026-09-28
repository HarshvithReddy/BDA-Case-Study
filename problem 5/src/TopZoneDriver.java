import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopZoneDriver {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "VoltGrid Problem 5 - Find Top Zones");

        job.setJarByClass(TopZoneDriver.class);

        job.setMapperClass(TopZoneMapper.class);
        job.setReducerClass(TopZoneReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(
            job, new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
            job, new Path(args[1])
        );

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}