package cs455.hadoop.util;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IntSortMapper extends Mapper< Text, Text, IntWritable, Text> {

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        try {
            //System.out.printf("key=%s, value=%s%n", key.toString(), value.toString());
            int newVal = Integer.parseInt(value.toString());
            IntWritable newIW = new IntWritable(newVal);
            context.write(newIW, key);

        } catch (IOException e) {
            //System.out.printf("***SORT_MAPPER_ERROR: key='%s', value='%s'%n", key.toString(), value.toString());
            //e.printStackTrace();
            throw e;
        }
    }
}
