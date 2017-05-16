package com.bigdata.train.hadoop.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TestPartionerMapper extends Mapper<LongWritable, Text, LongWritable, Text>{  

	private final static IntWritable one = new IntWritable(1);  
	private Text word = new Text();  
 
	public void map(LongWritable key, Text value, Context context 
			) throws IOException, InterruptedException {  
 
		String line = value.toString();
		String newLine = line.replace(" ", "\t");
		
		context.write(key, new Text(newLine));
	}
}
