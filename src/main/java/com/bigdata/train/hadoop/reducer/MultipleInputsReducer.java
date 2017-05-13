package com.bigdata.train.hadoop.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.bigdata.train.hadoop.bean.UserBean;

public class MultipleInputsReducer extends
		Reducer<Text, UserBean, Text, UserBean> {
	
	private UserBean userBean = new UserBean();
	
	public void reduce (Text key, Iterable<UserBean> values, Context context) 
			throws IOException, InterruptedException {
		double totalIncome = 0;
		double totalExpense = 0;
		
		for (UserBean userBean : values) {
			totalIncome += userBean.getIncome();
			totalExpense += userBean.getExpense();
		}
		userBean.setFields(key.toString(), totalIncome,totalExpense);
		
		context.write(key, userBean);
	}
}
