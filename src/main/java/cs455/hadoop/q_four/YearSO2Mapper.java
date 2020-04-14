package cs455.hadoop.q_four;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class YearSO2Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitLine = value.toString().replace("\"", "").split(",");

        try {
            String year = splitLine[11].substring(0, 4);
            double quality = Double.parseDouble(splitLine[13]);

            System.out.printf("Year: %s, quality: %f%n", year, quality);

            context.write(new Text(year), new DoubleWritable(quality));

        } catch (NumberFormatException ignored) {
            System.out.println("FOUND EXCEPTION: " + splitLine[12]);
            //System.out.println("IGNORING: " + splitLine[13] + ", " + splitLine[0]);
        }
    }
}
