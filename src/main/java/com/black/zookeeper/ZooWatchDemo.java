package com.black.zookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**  
 * @Author : Erich ErichLee@qq.com    
 * @Date   : 2019年3月11日 
 * @Comment: 
 *            
 */
public class ZooWatchDemo {


    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        String connected = "192.168.100.120:2181,192.168.100.121:2181,192.168.100.122:2181,192.168.100.123:2181";
        //毫秒
        int timeout = 2000;

        //1.连接zk集群
        ZooKeeper zkCli = new ZooKeeper(connected, timeout, new Watcher() {
            //监听回调
            public void process(WatchedEvent watchedEvent) {
                System.out.println("正在监听中......");
            }
        });

        //2.监听 ls / watch get /watch
        List<String> children = zkCli.getChildren("/", new Watcher() {
            public void process(WatchedEvent watchedEvent) {

                System.out.println("此时监听的路径是：" + watchedEvent.getPath());
                System.out.println("此时监听的类型为：" + watchedEvent.getType());
                System.out.println("有人正在修改数据！！");

            }
        }, null);

        Thread.sleep(Long.MAX_VALUE);

    }
}
