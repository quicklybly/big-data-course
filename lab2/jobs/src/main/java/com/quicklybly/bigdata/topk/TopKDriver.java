package com.quicklybly.bigdata.topk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.UUID;

/**
 * Usage: yarn jar topk-jobs.jar [-D prop=value] input output [k] [reducers] [useCombiner] [cleanTmp]
 */
public class TopKDriver extends Configured implements Tool {

    public static final String K_PROPERTY = "topk.k";

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new TopKDriver(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: TopKDriver [-D prop=value] input output [k] [reducers] [useCombiner] [cleanTmp]");
            return 2;
        }
        var input = new Path(args[0]);
        var output = new Path(args[1]);
        int k = args.length > 2 ? Integer.parseInt(args[2]) : 10;
        int numberOfReduceTasks = args.length > 3 ? Integer.parseInt(args[3]) : 1;
        boolean useCombiner = args.length <= 4 || Boolean.parseBoolean(args[4]);
        boolean cleanTmp = args.length <= 5 || Boolean.parseBoolean(args[5]);

        Configuration conf = getConf();
        conf.setInt(K_PROPERTY, k);

        var tempWordCount = new Path("/tmp/topk-wordcount-" + UUID.randomUUID());
        System.out.println("tempWordCount: " + tempWordCount);

        try {
            try (var wordCount = wordCountJob(conf, input, tempWordCount, numberOfReduceTasks, useCombiner)) {
                if (!wordCount.waitForCompletion(true)) {
                    return 1;
                }
            }
            try (var topK = topKJob(conf, tempWordCount, output)) {
                return topK.waitForCompletion(true) ? 0 : 1;
            }
        } finally {
            if (cleanTmp) {
                FileSystem.get(conf).delete(tempWordCount, true);
            }
        }
    }

    private Job wordCountJob(Configuration conf, Path input, Path output,
                             int numberOfReduceTasks, boolean useCombiner) throws Exception {
        Job job = Job.getInstance(conf, "top-k: word count");
        job.setJarByClass(TopKDriver.class);

        job.setMapperClass(TokenizeMapper.class);
        if (useCombiner) {
            job.setCombinerClass(SumReducer.class);
        }
        job.setReducerClass(SumReducer.class);
        job.setNumReduceTasks(numberOfReduceTasks);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    private Job topKJob(Configuration conf, Path input, Path output) throws Exception {
        Job job = Job.getInstance(conf, "top-k: select");
        job.setJarByClass(TopKDriver.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(TopKMapper.class);
        job.setReducerClass(TopKReducer.class);

        job.setMapOutputKeyClass(CountWordKey.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }
}
