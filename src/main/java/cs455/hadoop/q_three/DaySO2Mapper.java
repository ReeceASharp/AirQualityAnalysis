package cs455.hadoop.q_three;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DaySO2Mapper extends Mapper <LongWritable, Text, Text, DoubleWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitLine = value.toString().replace("\"", "").split(",");

        try {
            boolean lastCentury = Integer.parseInt(splitLine[11].substring(0, 4)) - 2000 < 0;
            if (!lastCentury) {

                double quality = Double.parseDouble(splitLine[13]);
                String hour = splitLine[12];

                System.out.printf("Hour: %s, quality: %f%n", hour, quality);

                context.write(new Text(hour), new DoubleWritable(quality));
            }
            else {
                System.out.println("NOT");
            }
        } catch (NumberFormatException ignored) {
            System.out.println("FOUND EXCEPTION: " + splitLine[12]);
            //System.out.println("IGNORING: " + splitLine[13] + ", " + splitLine[0]);
        }
    }
}
