package com.bigdata.train.hadoop.partioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartioner extends Partitioner<LongWritable, Text> {

	@Override
	public int getPartition(LongWritable key, Text value, int numPartitions) {
		String line = value.toString();
		
		if(line.contains("神州行卡")) {
			return 0;
		} else {
			return 1;
		}
	}

}
