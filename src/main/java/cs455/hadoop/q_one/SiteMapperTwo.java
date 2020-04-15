package cs455.hadoop.q_one;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class SiteMapperTwo extends Mapper<Text, Text, Text, IntWritable> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        try {
            //System.out.printf("SITE MAPPER 2 key='%s', value='%s'%n", key.toString(), value.toString());
            //get string, and convert to array, we know the elements that we care about
            //System.out.printf("MAPPER_TWO: key: %s, value: %s%n", key.toString(), value.toString());
            String[] vals = key.toString().split(",");

            //Key = State, value = unique site
            context.write(new Text(vals[0]), new IntWritable(1));
        } catch (IOException e) {
                //System.out.printf("***SITE MAPPER 2 ERROR: key='%s', value='%s'%n", key.toString(), value.toString());
                //e.printStackTrace();
                throw e;
        }
    }
}