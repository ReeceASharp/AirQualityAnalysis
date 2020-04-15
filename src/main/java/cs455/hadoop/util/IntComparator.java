package cs455.hadoop.util;

import java.nio.ByteBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;

//used to reorder the output data values in the form of int from other jobs into descending order
//used by Q1
public class IntComparator extends WritableComparator {

    public IntComparator() {
        super(IntWritable.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {

        Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
        Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

        return v1.compareTo(v2) * (-1);
    }
}