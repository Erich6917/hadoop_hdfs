package com.black.hadoop.mapreduce.workbook.demo01;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @Author : Erich ErichLee@qq.com
 * @Date : 2019年3月14日
 * @Comment :
 * 
 */
public class CourseBean2 implements WritableComparable<CourseBean2> {
	private String courseName;
	private String userName;
	private float courseAvg;
	private long courseMax;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(courseName);
		out.writeUTF(userName);
		out.writeFloat(courseAvg);
		out.writeLong(courseMax);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		courseName = in.readUTF();
		userName = in.readUTF();
		courseAvg = in.readFloat();
		courseMax = in.readLong();
	}

	@Override
	public int compareTo(CourseBean2 o) {
		int index = this.courseName.compareTo(o.getCourseName());
		if (index == 0) {
			return this.courseMax > o.getCourseMax() ? -1 : 1;
		}

		return this.courseMax > o.getCourseMax() ? 1 : -1;
	}

	@Override
	public String toString() {
		return String.format("[%10s\t%16s\t%s\t%s]", courseName, userName, courseAvg,courseMax);
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

	public long getCourseMax() {
		return courseMax;
	}

	public void setCourseMax(long courseMax) {
		this.courseMax = courseMax;
	}

	public CourseBean2() {
		super();
	}

	public CourseBean2(String courseName, String userName, float courseAvg, long courseMax) {
		super();
		this.courseName = courseName;
		this.userName = userName;
		this.courseAvg = courseAvg;
		this.courseMax = courseMax;
	}

}
