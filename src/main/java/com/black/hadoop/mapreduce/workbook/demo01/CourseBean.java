package com.black.hadoop.mapreduce.workbook.demo01;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @Author	 : Erich ErichLee@qq.com
 * @Date	 : 2019年3月14日
 * @Comment	 :
 * 
 */
public class CourseBean implements WritableComparable<CourseBean> {
	private String courseName;
	private String userName;
	private float courseAvg;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(courseName);
		out.writeUTF(userName);
		out.writeFloat(courseAvg);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		courseName = in.readUTF();
		userName = in.readUTF();
		courseAvg = in.readFloat();
	}

	@Override
	public int compareTo(CourseBean o) {
		return this.courseAvg > o.getCourseAvg() ? -1:1;
	}
	
	
	@Override
	public String toString() {
		return String.format("[%10s\t%16s\t%s]",courseName,userName,courseAvg);
//		return "CourseBean [courseName=" + courseName + ", userName=" + userName + ", courseAvg=" + courseAvg + "]";
	}

	public String getCourseName() {
		return courseName;
	}

	public void setCourseName(String courseName) {
		this.courseName = courseName;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public float getCourseAvg() {
		return courseAvg;
	}

	public void setCourseAvg(float courseAvg) {
		this.courseAvg = courseAvg;
	}

	public CourseBean() {
		super();
	}

	public CourseBean(String courseName, String userName, float courseAvg) {
		super();
		this.courseName = courseName;
		this.userName = userName;
		this.courseAvg = courseAvg;
	}

}
