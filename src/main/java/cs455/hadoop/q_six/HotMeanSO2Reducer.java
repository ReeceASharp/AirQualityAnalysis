package cs455.hadoop.q_six;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HotMeanSO2Reducer extends Reducer<Text, DoubleWritable,Text, DoubleWritable > {
    public void reduce(Text key,  Iterable<DoubleWritable> values, Context context) throws IOException,                                                       InterruptedException
    {
        //System.out.printf("REDUCING: text: %s %n", key);
        //initial values
        double totalCO2 = 0;
        int count = 0;

        //add up all of the recorded values for EAST/WEST, then average it out
        //unfortunately, this only reduces twice, which could be a ton of data
        for (DoubleWritable value : values)
        {
            totalCO2 += value.get();
            count += 1;
        }
        context.write(key, new DoubleWritable(totalCO2/count));
    }
}