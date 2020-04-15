package cs455.hadoop.util;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DoubleSortMapper extends Mapper<Text, Text, DoubleWritable, Text> {

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        try {
            //System.out.printf("key=%s, value=%s%n", key.toString(), value.toString());
            double newVal = Double.parseDouble(value.toString());
            DoubleWritable newIW = new DoubleWritable(newVal);
            context.write(newIW, key);

        } catch (IOException e) {
            //System.out.printf("***SORT_MAPPER_ERROR: key='%s', value='%s'%n", key.toString(), value.toString());
            //e.printStackTrace();
            throw e;
        }
    }
}
