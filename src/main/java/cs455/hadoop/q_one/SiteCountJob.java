package cs455.hadoop.q_one;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
/*
    /s/indianapolis/a/nobackup/cs455/chipmunk (DFS location)
    /s/indianapolis/a/tmp/chipmunk/  (LOG LOCATION)
 */

/**
 * This is the main class. Hadoop will invoke the main method of this class.
 */
public class SiteCountJob {
    public static void main(String[] args) {
        try {

            // create JobControl to daisychain jobs
            JobControl jobControl = new JobControl("jobChain");

            //*********************
            //  ONE SECOND JOB
            //*********************
            Configuration conf1 = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn webapp.
            Job job1 = Job.getInstance(conf1, "word count");

            FileInputFormat.addInputPath(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));

            // Current class.
            job1.setJarByClass(SiteCountJob.class);

            //Mapper, Combiner, Reducer
            job1.setMapperClass(SiteMapperOne.class);
            job1.setCombinerClass(SiteReducerOne.class);
            job1.setReducerClass(SiteReducerOne.class);

            // Outputs from the Mapper.
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(IntWritable.class);
            // Outputs from Reducer. It is sufficient to set only the following two properties
            // if the Mapper and Reducer has same key and value types. It is set separately for
            // elaboration.
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(IntWritable.class);

            ControlledJob controlledJob1 = new ControlledJob(conf1);
            controlledJob1.setJob(job1);

            //*********************
            //  START SECOND JOB
            //*********************
            Configuration conf2 = new Configuration();

            Job job2 = Job.getInstance(conf2);
            job2.setJarByClass(SiteCountJob.class);
            job2.setJobName("Word Invert");

            FileInputFormat.setInputPaths(job2, new Path(args[1] + "/temp"));
            FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));

            job2.setMapperClass(SiteMapperTwo.class);
            job2.setCombinerClass(SiteReducerTwo.class);
            job2.setReducerClass(SiteReducerTwo.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);
            job2.setInputFormatClass(KeyValueTextInputFormat.class);

            ControlledJob controlledJob2 = new ControlledJob(conf2);
            controlledJob2.setJob(job2);

            // make job2 dependent on job1, will only run when job1 is done
            controlledJob2.addDependingJob(controlledJob1);
            // add the job to the job control
            jobControl.addJob(controlledJob2);


            Thread jobControlThread = new Thread(jobControl);
            jobControlThread.start();

            job1.waitForCompletion(true);
            // Block until the job is completed.
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
