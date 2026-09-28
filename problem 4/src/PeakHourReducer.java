import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class PeakHourReducer
        extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context)
            throws IOException, InterruptedException {

        double maxConsumption = 0;

        for (DoubleWritable value : values) {

            if (value.get() > maxConsumption) {
                maxConsumption = value.get();
            }
        }

        context.write(key, new DoubleWritable(maxConsumption));
    }
}