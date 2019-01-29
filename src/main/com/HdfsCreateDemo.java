package main.com;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
public class HdfsCreateDemo {

	public void ls() throws IOException {

        System.setProperty("HADOOP_USER_NAME","root");
        //step 1���ò�����ָ��namenode��ַ

        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","hdfs://172.16.22.40:9000");
        //step 2 �����ͻ���
        FileSystem client=FileSystem.get(conf);
        //step 3 ����Ŀ¼
        client.mkdirs(new Path("/sp"));
        client.close();
        System.out.println("Successful");
	}

	public static void main(String[] args) throws IOException {
		HdfsCreateDemo demo = new HdfsCreateDemo();

		demo.ls();
	}
}
