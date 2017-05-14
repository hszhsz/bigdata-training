package com.bigdata.train.hadoop.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bigdata.train.hadoop.bean.UserBean;
import com.bigdata.train.hadoop.mapper.MultipleInputsMapper1;
import com.bigdata.train.hadoop.mapper.MultipleInputsMapper2;
import com.bigdata.train.hadoop.reducer.MultipleInputsReducer;

public class MultipleInputsJob extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration config = getConf();
		
	    // 校验命令行输入参数  
	    if (args.length < 3) {  
	    	System.err.println("Usage: MultipleInput <in> [<in>...] <out>");  
	    	System.exit(2);  
	    }  
		
	    Job job = Job.getInstance(config, "Multiple Inputs");
	    job.setJarByClass(MultipleInputsJob.class);
	    
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(UserBean.class);

        job.setReducerClass(MultipleInputsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(UserBean.class);
        job.setNumReduceTasks(3);
        
        MultipleInputs.addInputPath(job, new Path(args[0]),
        		TextInputFormat.class, MultipleInputsMapper1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), 
        		TextInputFormat.class, MultipleInputsMapper2.class);
        
        Path outPath = new Path(args[2]);
        
        FileSystem fs = FileSystem.get(config);
        if(fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        
        FileOutputFormat.setOutputPath(job, outPath);

        return job.waitForCompletion(true) ? 0:-1;
	}
	
	public static void main(String args[]) throws Exception{
		System.setProperty("hadoop.home.dir", 
				"G:\\Downloads\\hadoop-2.6.5\\hadoop-2.6.5");

    	int exitCode = ToolRunner.run(new MultipleInputsJob(),args);
        System.exit(exitCode);
	}
}
