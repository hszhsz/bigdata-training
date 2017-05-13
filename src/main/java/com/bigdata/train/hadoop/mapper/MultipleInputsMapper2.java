package com.bigdata.train.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.bigdata.train.hadoop.bean.UserBean;

public class MultipleInputsMapper2 extends
		Mapper<LongWritable, Text, Text, UserBean> {
	
	private UserBean userBean = new UserBean();
	private Text k = new Text();
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String line = value.toString();
		String fields[] = line.split(",");
		
		if(fields.length < 3) {
			System.err.println("invalid line");
		} else {
			userBean.setAccount(fields[0]);
			userBean.setIncome(Double.parseDouble(fields[1]));
			userBean.setIncome(Double.parseDouble(fields[2]));
			
			k.set(fields[0]);
			context.write(k, userBean);
		}
	}
}
