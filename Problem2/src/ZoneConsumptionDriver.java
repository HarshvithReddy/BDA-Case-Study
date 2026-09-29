```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ZoneConsumptionDriver {

    public static void main(String[] args)
            throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "Problem 2 - Zone Consumption"
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

        // IMPORTANT: Force one reducer
        job.setNumReduceTasks(1);

        // Mapper output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input
        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );

        // Output
        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );

        // Distributed Cache: zones.csv
        job.addCacheFile(
                new Path(args[2]).toUri()
        );

        // Run job
        System.exit(
                job.waitForCompletion(true)
                        ? 0 : 1
        );
    }
}
```
