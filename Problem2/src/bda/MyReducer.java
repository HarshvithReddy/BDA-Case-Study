import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        double maxPercentage = 0.0;

        for (DoubleWritable value : values) {

            if (value.get() > maxPercentage) {
                maxPercentage = value.get();
            }
        }

        context.write(key, new DoubleWritable(maxPercentage));
    }
}
