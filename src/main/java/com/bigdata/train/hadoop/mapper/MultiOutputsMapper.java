package com.bigdata.train.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MultiOutputsMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {
	public void map(LongWritable key, Text value,Context context) 
			throws IOException, InterruptedException {
		String line = value.toString();
		
		if(null != line && line.length() > 0) {
			String [] fields = line.split(",");
			
			context.write(new IntWritable(Integer.parseInt(fields[0])),value);
		}
	}
}
