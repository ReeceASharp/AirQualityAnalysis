package cs455.hadoop.q_six;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HotMeanSO2Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //organize information from csv into an array
        String[] splitLine = value.toString().replace("\"", "").split(",");

        /*
        Top Average Temperatures
        **************************
        85.23620301755703	Arizona
        83.1956953445332	Texas
        82.9357151309137	Nevada
        81.71553120756649	Mississippi
        81.11059967866603	Florida
        81.10992415770546	Louisiana
        80.96621147874862	Arkansas
        80.16672938214398	Oklahoma
        79.81845587787511	New Mexico
        78.85170028690288	South Carolina
        **************************
         */

        boolean hotState = false;

        try {
            switch (Integer.parseInt(splitLine[0])) {
                case 4:     //AZ
                case 5:     //AR
                case 12:    //FL
                case 22:    //LA
                case 28:    //MS
                case 32:    //NV
                case 35:    //NM
                case 40:    //OK
                case 45:    //SC
                case 48:    //TX
                    hotState = true;
                    break;
            }

            if (hotState) {
                double quality = Double.parseDouble(splitLine[13]);
                context.write(new Text(splitLine[21]), new DoubleWritable(quality));
            }

        } catch (NumberFormatException ignored) {
            //System.out.println("FOUND EXCEPTION: " + splitLine[12]);
            //System.out.println("IGNORING: " + splitLine[13] + ", " + splitLine[0]);
        }

    }
}
