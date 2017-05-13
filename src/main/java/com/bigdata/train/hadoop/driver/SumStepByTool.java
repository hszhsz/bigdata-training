package com.bigdata.train.hadoop.driver;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bigdata.train.hadoop.bean.UserBean;

public class SumStepByTool extends Configured implements Tool{

    public static class SumStepByToolMapper extends Mapper<LongWritable, Text, Text, UserBean>{

        private UserBean outBean = new UserBean();
        private Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

            String line = value.toString();
            String[] fields = line.split("\t");
            System.err.println("fields length: " + fields.length);
            String account = fields[0];
            double income = Double.parseDouble(fields[1]);
            double expense = Double.parseDouble(fields[2]);

            outBean.setFields(account, income, expense);
            k.set(account);

            context.write(k, outBean);
        }
    }

    public static class SumStepByToolWithCommaMapper extends Mapper<LongWritable, Text, Text, UserBean>{

            private UserBean outBean = new UserBean();
            private Text k = new Text();

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

                String line = value.toString();
                String[] fields = line.split(",");

                String account = fields[0];
                double income = Double.parseDouble(fields[1]);
                double expense = Double.parseDouble(fields[2]);

                outBean.setFields(account, income, expense);
                k.set(account);

                context.write(k, outBean);
            }
        }

    public static class SumStepByToolReducer extends Reducer<Text, UserBean, Text, UserBean>{

        private UserBean outBean = new UserBean();
        @Override
        protected void reduce(Text key, Iterable<UserBean> values, Context context) throws IOException, InterruptedException{
            double income_sum = 0;
            double expense_sum = 0;

            for(UserBean infoBeanMy : values)
            {
                income_sum += infoBeanMy.getIncome();
                expense_sum += infoBeanMy.getExpense();
            }
            outBean.setFields("", income_sum, expense_sum);
            context.write(key, outBean);
        }

    }


    public static class SumStepByToolPartitioner extends Partitioner<Text, UserBean>{

        private static Map<String, Integer> accountMap = new HashMap<String, Integer>(); 

        static {
            accountMap.put("zhangsan", 1);
            accountMap.put("lisi", 2);
            accountMap.put("wangwu", 3);
        }

        @Override
        public int getPartition(Text key, UserBean value, int numPartitions) {
            String keyString = key.toString();
            String name = keyString.substring(0, keyString.indexOf("@"));
            Integer part = accountMap.get(name);
            if (part == null )
            {
                part = 0;
            }
            return part;
        }

    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        //conf.setInt("mapreduce.input.lineinputformat.linespermap", 2);
        Job job = Job.getInstance(conf);
        job.setJarByClass(this.getClass());
        job.setJobName("SumStepByTool");

        //job.setInputFormatClass(TextInputFormat.class); //这个是默认的输入格式
        //job.setInputFormatClass(KeyValueTextInputFormat.class); //这个把一行记录的第一个区域当做key，其他区域作为value
        //job.setInputFormatClass(NLineInputFormat.class);

//      job.setMapperClass(SumStepByToolMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(UserBean.class);

        job.setReducerClass(SumStepByToolReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(UserBean.class);
        job.setNumReduceTasks(3);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, SumStepByToolMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SumStepByToolWithCommaMapper.class);
//      FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));


        return job.waitForCompletion(true) ? 0:-1;
    }

    public static void main(String[] args) throws Exception {
    	System.setProperty("hadoop.home.dir", 
				"G:\\Downloads\\hadoop-2.6.5\\hadoop-2.6.5");

    	int exitCode = ToolRunner.run(new SumStepByTool(),args);
        System.exit(exitCode);
    }
}