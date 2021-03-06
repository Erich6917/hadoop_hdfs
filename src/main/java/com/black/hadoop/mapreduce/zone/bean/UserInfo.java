package com.black.hadoop.mapreduce.zone.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


/**
 * @Author : Erich ErichLee@qq.com
 * @Date : 2019年3月4日
 * @Comment:
 * 
 */
public class UserInfo implements WritableComparable<UserInfo> {


	private String name;
	private String id;
	private String date;
	private long flowUp;
	private long flowDown;
	private long flowTotal;

	public UserInfo() {
		super();
	}

	public UserInfo(String name, String id, String date, long flowUp, long flowDown) {
		super();
		this.name = name;
		this.id = id;
		this.date = date;
		this.flowUp = flowUp;
		this.flowDown = flowDown;
		this.flowTotal = flowUp + flowDown;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public long getFlowUp() {
		return flowUp;
	}

	public void setFlowUp(long flowUp) {
		this.flowUp = flowUp;
	}

	public long getFlowDown() {
		return flowDown;
	}

	public void setFlowDown(long flowDown) {
		this.flowDown = flowDown;
	}

	public long getFlowTotal() {
		return flowTotal;
	}

	public void setFlowTotal(long flowTotal) {
		this.flowTotal = flowTotal;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeUTF(id);
		out.writeUTF(date);
		out.writeLong(flowUp);
		out.writeLong(flowDown);
		out.writeLong(flowTotal);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		name = in.readUTF();
		id = in.readUTF();
		date = in.readUTF();
		flowUp = in.readLong();
		flowDown = in.readLong();
		flowTotal = in.readLong();
		
	}


	
//	@Override
//	public boolean equals(Object obj) {
//		if (this == obj)
//			return true;
//		if (obj == null)
//			return false;
//		if (getClass() != obj.getClass())
//			return false;
//		UserInfo other = (UserInfo) obj;
//		if (id == null) {
//			if (other.id != null)
//				return false;
//		} else if (!id.equals(other.id))
//			return false;
//		return true;
//	}
//
//	@Override
//	public int hashCode() {
//		final int prime = 31;
//		int result = 1;
//		result = prime * result + ((id == null) ? 0 : id.hashCode());
//		return result;
//	}

	
	

	@Override
	public String toString() {
		return String.format("%8s %6s %5d %5d %5d", name, date, flowUp, flowDown, flowTotal);
	}
	
	@Override
	public int compareTo(UserInfo o) {

		// 比较流量大小 倒序
		return this.flowTotal > o.getFlowTotal() ? -1 : 1;
//		return 0;
	}
	

}
