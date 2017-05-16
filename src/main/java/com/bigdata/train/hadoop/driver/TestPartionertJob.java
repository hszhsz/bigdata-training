package com.bigdata.train.hadoop.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bigdata.train.hadoop.mapper.TestPartionerMapper;
import com.bigdata.train.hadoop.mapper.WordCountMapper;
import com.bigdata.train.hadoop.partioner.MyPartioner;
import com.bigdata.train.hadoop.reducer.TestPartionerReducer;
import com.bigdata.train.hadoop.reducer.WordCountReducer;

public class TestPartionertJob {
	public static void main(String args[]) {
		
		System.setProperty("hadoop.home.dir", 
				"G:\\Downloads\\hadoop-2.6.5\\hadoop-2.6.5");
		
	    Configuration conf = new Configuration();
	      
	    // 校验命令行输入参数  
	    if (args.length < 2) {  
	      System.err.println("Usage: wordcount <in> [<in>...] <out>");  
	      System.exit(2);  
	    }  
	    try {
		    // 构造一个Job实例job，并命名为"word count"  
		    Job job = Job.getInstance(conf, "test partioner");
		      
		    // 设置jar  
		    job.setJarByClass(TestPartionertJob.class);  
		      
		    // 设置Mapper  
		    job.setMapperClass(TestPartionerMapper.class); 
		    job.setMapOutputKeyClass(LongWritable.class);
	        job.setMapOutputValueClass(Text.class);
		    // 设置Combiner  
		//    job.setCombinerClass(WordCountReducer.class);  
		    
		    //设置partioner
		    job.setPartitionerClass(MyPartioner.class);
		    job.setNumReduceTasks(2);
		    // 设置Reducer  
		    job.setReducerClass(TestPartionerReducer.class);  
		    // 设置OutputKey  
		    job.setOutputKeyClass(NullWritable.class);  
		    // 设置OutputValue  
		    job.setOutputValueClass(Text.class);  
		      
		    // 添加输入路径  
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    
		    Path outPath = new Path(args[1]); 
		    
	        FileSystem fs = FileSystem.get(conf);
	        if(fs.exists(outPath)) {
	            fs.delete(outPath, true);
	        }
		    // 添加输出路径  
		    FileOutputFormat.setOutputPath(job, outPath);
		    
		    // 等待作业job运行完成并退出  
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	    } catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
