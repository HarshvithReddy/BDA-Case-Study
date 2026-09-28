import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class TopZoneReducer
        extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context)
            throws IOException, InterruptedException {

        double total = 0;

        for (DoubleWritable value : values) {
            total += value.get();
        }

        context.write(key, new DoubleWritable(total));
    }
}