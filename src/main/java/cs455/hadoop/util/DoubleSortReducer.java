package cs455.hadoop.util;


import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//just spits out the data as is (theoretically not even needed, could be specified in the
//driver function
public class DoubleSortReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {

    Text word = new Text();

    @Override
    public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {
            word.set(value);
            context.write(key, word);
        }
    }
}