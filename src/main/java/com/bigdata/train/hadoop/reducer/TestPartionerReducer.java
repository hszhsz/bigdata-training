package com.bigdata.train.hadoop.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TestPartionerReducer extends 
	Reducer<LongWritable,Text,NullWritable,Text> {

	public void reduce(LongWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		for(Text text : values) {
			context.write(NullWritable.get(), text);
		}
	}  
}
