package cs455.hadoop.util;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class IntSortReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    Text word = new Text();

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {
            word.set(value);
            context.write(key, word);
        }
    }
}
