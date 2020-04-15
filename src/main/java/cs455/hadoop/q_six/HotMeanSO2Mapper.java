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
        Arizona	85.23620301755703
        Texas	83.1956953445332
        Nevada	82.93571513091376
        Mississippi	81.71553120756653
        Florida	81.11059967866603
        Louisiana	81.10992415770541
        Arkansas	80.96621147874862
        Oklahoma	80.16672938214376
        New Mexico	79.81845587787511
        South Carolina	78.8517002869028
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
