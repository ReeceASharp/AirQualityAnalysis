package cs455.hadoop.q_two;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EWMapperOne  extends Mapper <LongWritable, Text, Text, DoubleWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException
    {
        //Data was obtained from an epa states_and_countries.csv sheet obtained from their website
        //https://aqs.epa.gov/aqsweb/documents/codetables/states_and_counties.html
        String[] splitLine = value.toString().replace("\"", "").split(",");

        try {
            //element 13 is the measurement of CO2
            double quality = Double.parseDouble(splitLine[13]);
            //Only care about EAST/WEST, so we don't need to compare strings
            //use state int value instead, much faster
            //ignore states that aren't part of the east, or west coast
            int state = Integer.parseInt(splitLine[0]);

            //System.out.printf("QUALITY: %f, State: %d%n", quality, state);
            switch(state) {
                //WEST COAST
                case 6:     //CA
                case 41:    //OR
                case 52:    //WA
                case 2:     //AK
                    context.write(new Text("WEST"),new DoubleWritable(quality));
                    break;
                //EAST COAST
                case 23:    //ME
                case 33:    //NH
                case 25:    //MA
                case 44:    //RI
                case 9:     //CT
                case 36:    //NY
                case 10:    //DE
                case 24:    //MD
                case 51:    //VA
                case 37:    //NC
                case 45:    //SC
                case 13:    //GA
                case 12:    //Fl
                case 42:    //PA
                case 11:    //DC
                    context.write(new Text("EAST"), new DoubleWritable(quality));
                    break;
            }


        } catch (NumberFormatException ignored) {
            //System.out.println("IGNORING: " + splitLine[13] + ", " + splitLine[0]);
        }


    }
}

