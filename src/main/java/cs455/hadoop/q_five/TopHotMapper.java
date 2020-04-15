package cs455.hadoop.q_five;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TopHotMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //organize information from csv into an array
        String[] splitLine = value.toString().replace("\"", "").split(",");
        try {
            //String month = splitLine[11].substring(5, 7);
            //YEAR-MONTH-DAY
            String[] dateValues = splitLine[11].split("-");
            boolean hotMonth = false;

            //attempt to grab decimal, if invalid, it errors out
            double temperature = Double.parseDouble(splitLine[13]);

            //will fail here if not valid #
            switch(Integer.parseInt(dateValues[1])) {
                case 6:
                case 7:
                case 8:
                    hotMonth = true;
                    break;
            }

            if (hotMonth) {
                //System.out.printf("O HOT | State: %s, Month: %s %n", splitLine[21], dateValues[1]);
                context.write(new Text(splitLine[21]), new DoubleWritable(temperature));

            } else {
                //System.out.printf("X not hot | Year: %s, quality: %f%n", splitLine[21], temperature);
            }

        } catch (NumberFormatException ignored) {
            //System.out.println("FOUND EXCEPTION: " + splitLine[12]);
            //System.out.println("IGNORING: " + splitLine[13] + ", " + splitLine[0]);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}