import java.io.*;
import java.util.HashMap;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class PeakHourMapper
        extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        if (line.startsWith("meterId"))
            return;

        String[] p = line.split(",");

        String zoneCode = p[1];
        String time = p[2].substring(11);
        double units = Double.parseDouble(p[3]);

        int hour = Integer.parseInt(time.substring(0, 2));

        if (hour >= 18 && hour <= 21) {
            context.write(
                new Text(zoneCode),
                new DoubleWritable(units)
            );
        }
    }
}