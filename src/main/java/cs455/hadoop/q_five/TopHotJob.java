package cs455.hadoop.q_five;

import cs455.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TopHotJob {
    public static void main(String[] args) {
        try {
            JobControl jobControl = new JobControl("jobChain");

            //*********************
            //  FIRST JOB
            //*********************

            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn webapp.
            Job job = Job.getInstance(conf, "Hottest Summer States");
            // Current class.
            job.setJarByClass(TopHotJob.class);
            // Mapper
            job.setMapperClass(TopHotMapper.class);
            // Reducer
            job.setReducerClass(TopHotReducer.class);
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            // Outputs from Reducer. It is sufficient to set only the following two properties
            // if the Mapper and Reducer has same key and value types. It is set separately for
            // elaboration.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(job, new Path(args[0]));
            // path to output in HDFS
            FileOutputFormat.setOutputPath(job, new Path(args[1] + "/unsorted"));

            ControlledJob controlledJob = new ControlledJob(conf);
            controlledJob.setJob(job);

            //*********************
            //  SECOND JOB
            //*********************
            // # sorts in descending order of temperatures

            Configuration conf2 = new Configuration();

            Job job2 = Job.getInstance(conf2);
            job2.setJarByClass(TopHotJob.class);
            job2.setJobName("Hottest States Sort");

            FileInputFormat.setInputPaths(job2, new Path(args[1] + "/unsorted"));
            FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));

            job2.setMapperClass(DoubleSortMapper.class);
            job2.setCombinerClass(DoubleSortReducer.class);
            job2.setReducerClass(DoubleSortReducer.class);

            job2.setMapOutputKeyClass(DoubleWritable.class);
            job2.setMapOutputValueClass(Text.class);

            job2.setOutputKeyClass(DoubleWritable.class);
            job2.setOutputValueClass(Text.class);
            job2.setInputFormatClass(KeyValueTextInputFormat.class);
            job2.setSortComparatorClass(DoubleComparator.class);

            ControlledJob controlledJob2 = new ControlledJob(conf2);
            controlledJob2.setJob(job2);

            // make job3 dependent on job2, will only run when job2 is done
            controlledJob2.addDependingJob(controlledJob2);
            // add the job to the job control
            jobControl.addJob(controlledJob2);

            //*********************
            //  JOB-CHAIN START
            //*********************

            Thread jobControlThread = new Thread(jobControl);
            jobControlThread.start();

            // Block until the job is completed.
            job.waitForCompletion(true);
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }

    }
}
