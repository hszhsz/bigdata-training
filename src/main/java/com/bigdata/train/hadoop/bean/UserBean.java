package com.bigdata.train.hadoop.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;

public class UserBean implements WritableComparable<BinaryComparable> {
	private String account;
	private double income;
	private double expense;
	
	public String getAccount() {
		return account;
	}
	public void setAccount(String account) {
		this.account = account;
	}
	public double getIncome() {
		return income;
	}
	public void setIncome(double income) {
		this.income = income;
	}
	public double getExpense() {
		return expense;
	}
	public void setExpense(double expense) {
		this.expense = expense;
	} 
	public void setFields(String account,double income, double expense) {
		setAccount(account);
		setIncome(income);
		setExpense(expense);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public int compareTo(BinaryComparable o) {
		// TODO Auto-generated method stub
		return 0;
	}
}
