import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Map<String, Double> sanctionedLoad = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {

        BufferedReader br = new BufferedReader(
            new FileReader("zones.csv")
        );

        String line;

        // Skip header
        br.readLine();

        while ((line = br.readLine()) != null) {

            String[] parts = line.split(",");

            String zoneCode = parts[0];
            double load = Double.parseDouble(parts[2]);

            sanctionedLoad.put(zoneCode, load);
        }

        br.close();
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip header
        if (line.startsWith("meterId")) {
            return;
        }

        String[] parts = line.split(",");

        String zoneCode = parts[1];
        double operatingLoad = Double.parseDouble(parts[3]);

        if (sanctionedLoad.containsKey(zoneCode)) {

            double sanctioned = sanctionedLoad.get(zoneCode);

            double percentage =
                    (operatingLoad / sanctioned) * 100.0;

            context.write(
                new Text(zoneCode),
                new DoubleWritable(percentage)
            );
        }
    }
}
