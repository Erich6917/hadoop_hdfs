package com.black.zookeeper;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;


 /**  
 * @Author : Erich ErichLee@qq.com    
 * @Date   : 2019年3月12日 
 * @Comment:  
 *            
 */  
public class ZkClient {

    private String connected = "192.168.100.120:2181,192.168.100.121:2181,192.168.100.122:2181,192.168.100.123:2181";
    //毫秒
    private int timeout = 2000;

    ZooKeeper zkCli = null;

    //连接zookeeper集群
    @Before
    public void init() throws IOException {
        //String:连接集群的ip端口 Int:超时设置 Wacher:监听
        zkCli = new ZooKeeper(connected, timeout, new Watcher() {
            //回调方法 显示/节点
            public void process(WatchedEvent watchedEvent) {
                List<String> chilren;

                //获得节点 get
                try {
                    chilren = zkCli.getChildren("/",true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    //测试 是否联通集群 创建节点
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        String p = zkCli.create("/bbq", "shaokao".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(p);
    }

    //查看子节点：ls /
    @Test
    public void getChild() throws KeeperException, InterruptedException {
        List<String> children = zkCli.getChildren("/", true);

        for(String c:children){
            System.out.println(c);
        }

    }

    //删除子节点数据:delete path
    @Test
    public void deleteData() throws KeeperException, InterruptedException {
        //-1表示所有版本
        zkCli.delete("/bbq",-1);
    }

    //修改数据：set path data
    @Test
    public void setData() throws KeeperException, InterruptedException {
        zkCli.setData("/itstar","who care".getBytes(),-1);

        //查看 /itstar
        byte[] data = zkCli.getData("/itstar", false, new Stat());
        System.out.println(new String(data));

    }

    //指定节点是否存在
    @Test
    public void testExist() throws KeeperException, InterruptedException {
        Stat exists = zkCli.exists("/itstar", false);

        System.out.println(exists == null ? "no have":"have");
    }



}
