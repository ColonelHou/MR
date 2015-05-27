package com.trace.jobs.pojo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PageVisitBean implements Writable{

	private long activity_count;
	private float avg_duration_count;
	private long sum_duration_count;
	public PageVisitBean() {
		super();
	}
	public PageVisitBean(long activity_count, float avg_duration_count,
			long sum_duration_count) {
		super();
		this.activity_count = activity_count;
		this.avg_duration_count = avg_duration_count;
		this.sum_duration_count = sum_duration_count;
	}
	public long getActivity_count() {
		return activity_count;
	}
	public void setActivity_count(long activity_count) {
		this.activity_count = activity_count;
	}
	public float getAvg_duration_count() {
		return avg_duration_count;
	}
	public void setAvg_duration_count(float avg_duration_count) {
		this.avg_duration_count = avg_duration_count;
	}
	public long getSum_duration_count() {
		return sum_duration_count;
	}
	public void setSum_duration_count(long sum_duration_count) {
		this.sum_duration_count = sum_duration_count;
	}
	@Override
	public String toString() {
		return activity_count
				+ "\t" + avg_duration_count
				+ "\t" + sum_duration_count;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(activity_count);
		out.writeFloat(avg_duration_count);
		out.writeLong(sum_duration_count);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		activity_count = in.readLong();
		avg_duration_count = in.readFloat();
		sum_duration_count = in.readLong();
	}
	
}
