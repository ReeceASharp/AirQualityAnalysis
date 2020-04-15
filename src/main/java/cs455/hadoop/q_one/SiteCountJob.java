package cs455.hadoop.q_one;

import cs455.hadoop.util.IntComparator;
import cs455.hadoop.util.IntSortMapper;
import cs455.hadoop.util.IntSortReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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
            //  FIRST JOB
            //*********************
            //counts up all of the

            Configuration conf1 = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn webapp.
            Job job1 = Job.getInstance(conf1, "State Site Counter");

            MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, SiteMapperOne.class);
            MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, SiteMapperOne.class);
            //FileInputFormat.addInputPath(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path(args[2] + "/temp"));

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
            //  SECOND JOB
            //*********************
            //condenses the unique sites into their respective states (very little work done)

            Configuration conf2 = new Configuration();

            Job job2 = Job.getInstance(conf2);
            job2.setJarByClass(SiteCountJob.class);
            job2.setJobName("Unique Site Condense");

            FileInputFormat.setInputPaths(job2, new Path(args[2] + "/temp"));
            FileOutputFormat.setOutputPath(job2, new Path(args[2] + "/unsorted_final"));

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

            //*********************
            //  THIRD JOB
            //*********************
            //sorts in descending order of # of sites

            Configuration conf3 = new Configuration();

            Job job3 = Job.getInstance(conf3);
            job3.setJarByClass(SiteCountJob.class);
            job3.setJobName("State Site Sort");

            FileInputFormat.setInputPaths(job3, new Path(args[2] + "/unsorted_final"));
            FileOutputFormat.setOutputPath(job3, new Path(args[2] + "/final"));

            job3.setMapperClass(IntSortMapper.class);
            job3.setCombinerClass(IntSortReducer.class);
            job3.setReducerClass(IntSortReducer.class);

            job3.setMapOutputKeyClass(IntWritable.class);
            job3.setMapOutputValueClass(Text.class);

            job3.setOutputKeyClass(IntWritable.class);
            job3.setOutputValueClass(Text.class);
            job3.setInputFormatClass(KeyValueTextInputFormat.class);
            job3.setSortComparatorClass(IntComparator.class);

            ControlledJob controlledJob3 = new ControlledJob(conf3);
            controlledJob2.setJob(job3);

            // make job3 dependent on job2, will only run when job2 is done
            controlledJob3.addDependingJob(controlledJob2);
            // add the job to the job control
            jobControl.addJob(controlledJob3);

            //*********************
            //  JOB-CHAIN START
            //*********************

            Thread jobControlThread = new Thread(jobControl);
            jobControlThread.start();


            // Block until the jobs is completed.
            job1.waitForCompletion(true);
            job2.waitForCompletion(true);
            System.exit(job3.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }

    }
}
