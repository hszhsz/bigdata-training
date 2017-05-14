package com.bigdata.train.hadoop.driver;

import java.io.IOException;  
  



import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.conf.Configured;  
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.DoubleWritable;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;  
import org.apache.hadoop.util.Tool;  
import org.apache.hadoop.util.ToolRunner;  
  
public class TestCombinerJob extends Configured implements Tool {  
  
    public static class MapClass extends Mapper<LongWritable,Text,Text,Text> {  
          
        static enum ClaimsCounters { MISSING, QUOTED };  
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  
            String fields[] = value.toString().split(",");  
            String country = fields[4];  
            String numClaims = fields[7];  
              
            if (numClaims.length() > 0 && !numClaims.startsWith("\"")) {  
                context.write(new Text(country), new Text(numClaims + ",1"));  
            }  
        }  
    }  
      
    public static class Reduce extends Reducer<Text,Text,Text,DoubleWritable> {  
          
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {  
            double sum = 0;  
            int count = 0;  
            for (Text value : values) {  
                String fields[] = value.toString().split(",");  
                sum += Double.parseDouble(fields[0]);  
                count += Integer.parseInt(fields[1]);  
            }  
            context.write(key, new DoubleWritable(sum/count));  
        }  
    }  
      
    public static class Combine extends Reducer<Text,Text,Text,Text> {  
          
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {  
            double sum = 0;  
            int count = 0;  
            for (Text value : values) {  
                String fields[] = value.toString().split(",");  
                sum += Double.parseDouble(fields[0]);  
                count += Integer.parseInt(fields[1]);  
            }  
            context.write(key, new Text(sum+","+count));  
        }  
    }  
      
    public int run(String[] args) throws Exception {
    	
		System.setProperty("hadoop.home.dir", 
				"G:\\Downloads\\hadoop-2.6.5\\hadoop-2.6.5");
    	
    	Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Test With Combiner");
        job.setJarByClass(TestCombinerJob.class);  
          
        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        Path outPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);  
          
        job.setMapperClass(MapClass.class);  
        job.setCombinerClass(Combine.class);  
        job.setReducerClass(Reduce.class);  
        job.setInputFormatClass(TextInputFormat.class);  
        job.setOutputFormatClass(TextOutputFormat.class);  
          
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(Text.class);  
          
        System.exit(job.waitForCompletion(true) ? 0 : 1);  
        return 0;  
    }  
      
    public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", 
				"G:\\Downloads\\hadoop-2.6.5\\hadoop-2.6.5");
		
        int res = ToolRunner.run(new Configuration(), new TestCombinerJob(), args);  
        System.exit(res);  
    }  
}  