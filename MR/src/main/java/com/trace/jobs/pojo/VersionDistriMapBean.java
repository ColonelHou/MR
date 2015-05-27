package com.trace.jobs.pojo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class VersionDistriMapBean implements Writable{

	
	private String application_ver;
	private String terminal_id;
	private long duration;
	private long day_session_count;
	public VersionDistriMapBean() {
		super();
	}
	public VersionDistriMapBean(String application_ver, String terminal_id,
			long duration, long day_session_count) {
		super();
		this.application_ver = application_ver;
		this.terminal_id = terminal_id;
		this.duration = duration;
		this.day_session_count = day_session_count;
	}
	public String getApplication_ver() {
		return application_ver;
	}
	public void setApplication_ver(String application_ver) {
		this.application_ver = application_ver;
	}
	public String getTerminal_id() {
		return terminal_id;
	}
	public void setTerminal_id(String terminal_id) {
		this.terminal_id = terminal_id;
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
				+ ", " + terminal_id + ", " + duration
				+ ", " + day_session_count ;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(application_ver);
		out.writeUTF(terminal_id);
		out.writeLong(duration);
		out.writeLong(day_session_count);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		application_ver = in.readUTF();
		terminal_id = in.readUTF();
		duration = in.readLong();
		day_session_count = in.readLong();
	}
	
}
