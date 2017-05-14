package com.bigdata.train.hadoop.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.bigdata.train.hadoop.mapper.MultiOutputsMapper;
import com.bigdata.train.hadoop.reducer.MultiOutputsReducer;

public class MultipleOutputsJob {
    public static void main(String[] args) throws Exception {
    	
		System.setProperty("hadoop.home.dir", 
				"G:\\Downloads\\hadoop-2.6.5\\hadoop-2.6.5");
    	
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "multiple outputs job");
        job.setJarByClass(MultipleOutputsJob.class);

        job.setMapperClass(MultiOutputsMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MultiOutputsReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        MultipleOutputs.addNamedOutput(job, "KeySpilt", 
        		TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "AllPart", 
        		TextOutputFormat.class, NullWritable.class, Text.class);

        Path outPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
