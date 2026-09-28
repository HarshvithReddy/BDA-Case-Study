import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class TrendMapper
        extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        if (line.startsWith("meterId"))
            return;

        String[] p = line.split(",");

        String zone = p[1];
        String date = p[2].substring(0, 10);
        double units = Double.parseDouble(p[3]);

        if (zone.equals("Z05") ||
            zone.equals("Z04") ||
            zone.equals("Z02")) {

            context.write(
                new Text(zone + "\t" + date),
                new DoubleWritable(units)
            );
        }
    }
}