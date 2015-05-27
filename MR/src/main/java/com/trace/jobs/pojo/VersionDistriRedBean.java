package com.trace.jobs.pojo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class VersionDistriRedBean implements Writable{
	
	private String application_ver;
	private long day_user_count;
	private long duration;
	private long day_session_count;
	public VersionDistriRedBean() {
		super();
	}
	public VersionDistriRedBean(String application_ver, long terminal_id,
			long duration, long day_session_count) {
		super();
		this.application_ver = application_ver;
		this.day_user_count = terminal_id;
		this.duration = duration;
		this.day_session_count = day_session_count;
	}
	
	public String getApplication_ver() {
		return application_ver;
	}
	public void setApplication_ver(String application_ver) {
		this.application_ver = application_ver;
	}
	public long getDay_user_count() {
		return day_user_count;
	}
	public void setDay_user_count(long day_user_count) {
		this.day_user_count = day_user_count;
	}
	public long getDuration() {
		return duration;
	}
	public void setDuration(long duration) {
		this.duration = duration;
	}
	public long getDay_session_count() {
		return day_session_count;
	}
	public void setDay_session_count(long day_session_count) {
		this.day_session_count = day_session_count;
	}
	@Override
	public String toString() {
		return "" + application_ver
				+ ", " + day_user_count + ", " + duration
				+ ", " + day_session_count ;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(application_ver);
		out.writeLong(day_user_count);
		out.writeLong(duration);
		out.writeLong(day_session_count);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		application_ver = in.readUTF();
		day_user_count = in.readLong();
		duration = in.readLong();
		day_session_count = in.readLong();
	}
}
