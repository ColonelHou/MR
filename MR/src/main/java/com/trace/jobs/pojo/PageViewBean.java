package com.trace.jobs.pojo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
//除了WritableComparable接口外，还有一个接口RawComparaotor。
//WritableComparable是需要把数据流反序列化为对象后，然后做对象之间的比较，
//而RawComparator是直接比较数据流的数据，不需要数据流反序列化成对象，省去了新建对象的开销。
import org.apache.hadoop.io.WritableComparable;


public class PageViewBean implements Writable{

	//private String activityname;
	private long activity_count;
	private float avg_duration_count;
	private long sum_duration_count;
	
	public float getAvg_duration_count() {
		return avg_duration_count;
	}
	public void setAvg_duration_count(float avg_duration_count) {
		this.avg_duration_count = avg_duration_count;
	}
	private int activityNum;
	
	
	public long getActivityNum() {
		return activityNum;
	}
	public void setActivityNum(int activityNum) {
		this.activityNum = activityNum;
	}
	@Override
	public String toString() {
		return "" + activityNum
				+ "\t" + avg_duration_count + "\t" + sum_duration_count + "\t" ;
	}
	public PageViewBean() {
		super();
	}
	public PageViewBean(long activity_count, long sum_duration_count) {
		super();
		this.activity_count = activity_count;
		this.sum_duration_count = sum_duration_count;
	}
	public long getActivity_count() {
		return activity_count;
	}
	public void setActivity_count(long activity_count) {
		this.activity_count = activity_count;
	}
	public long getSum_duration_count() {
		return sum_duration_count;
	}
	public void setSum_duration_count(long sum_duration_count) {
		this.sum_duration_count = sum_duration_count;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(activity_count);
		out.writeLong(sum_duration_count);
		out.writeInt(activityNum);
		out.writeFloat(avg_duration_count);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		activity_count = in.readLong();
		sum_duration_count = in.readLong();
		activityNum = in.readInt();
		avg_duration_count = in.readFloat();
	}
}
