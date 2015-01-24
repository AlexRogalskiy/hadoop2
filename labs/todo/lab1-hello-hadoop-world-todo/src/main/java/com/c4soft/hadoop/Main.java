package com.c4soft.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {
    public static final String JOB_NAME = "Main lab1";

    protected Main() {
    }

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(getConf(), JOB_NAME);

        // TODO: set Job jar

        // TODO: set Job mapper
        // TODO: set Job combiner (optional)
        // TODO: set Job reducer

        // TODO: set Job output key class
        // TODO: set Job output value class

        // TODO: set Job input path using FileInputFormat, Path and args[0]
        // TODO: set Job output path using FileInputFormat, Path and args[1]

        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new Main(), args);
        System.exit(status);
    }

}
