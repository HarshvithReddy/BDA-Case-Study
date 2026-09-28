import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class TopZoneMapper
        extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        if (line.startsWith("meterId"))
            return;

        String[] p = line.split(",");

        String zoneCode = p[1];
        double units = Double.parseDouble(p[3]);

        context.write(
            new Text(zoneCode),
            new DoubleWritable(units)
        );
    }
}
