package cs455.hadoop.q_one;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class SiteMapperOne extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //System.out.println("MAPPER ONE");
        //get string, and convert to array, we know the elements that we care about
        String[] vals = value.toString().replace("\"", "").split(",");

        String key_ = vals[21] + ',' + vals[1] + ',' + vals[2];

        //element 21 is the state string name
        context.write(new Text(key_), new IntWritable(1));
        
        // tokenize into words.
        //StringTokenizer itr = new StringTokenizer(value.toString());
        // emit word, count pairs.
        //while (itr.hasMoreTokens()) {
        //    context.write(new Text(itr.nextToken()), new IntWritable(1));
        // }
    }
}
