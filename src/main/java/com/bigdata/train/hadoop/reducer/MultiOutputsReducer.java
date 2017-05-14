package com.bigdata.train.hadoop.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MultiOutputsReducer extends
		Reducer<IntWritable, Text, NullWritable, Text> {
	
	private MultipleOutputs<NullWritable,Text> multiOutput = null;
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
        for(Text text : values) {
        	multiOutput.write("KeySpilt", NullWritable.get(), text, key.toString()+"/");
        	multiOutput.write("AllPart", NullWritable.get(), text);
        }
	}
	
	public void setup(Context context) {
		multiOutput = new MultipleOutputs<NullWritable, Text>(context);
	}
	
	public void cleanup(Context context) 
			throws IOException, InterruptedException {
		if (null != multiOutput) {
			multiOutput.close();
			multiOutput=null;
		}
 	}
}
