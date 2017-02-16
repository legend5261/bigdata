package zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * Created by pc on 2016/7/26.
 */
public class ZkClient {
    private static final int SESSION_TIMEOUT = 30000;

    private ZooKeeper zkClient;

    private Watcher wt = new Watcher() {
        public void process(WatchedEvent watchedEvent) {
            System.out.println("process : " + watchedEvent.getType());
        }
    };

    public void connect() throws Exception {
        zkClient = new ZooKeeper("jyibd60:2181", SESSION_TIMEOUT, null);
    }

    public void close() throws Exception {
        zkClient.close();
    }

    /**
     * 创建znode
     *
     * CreateMode.PERSISTENT 表示不会因为连接的断裂而删除节点
     */
    public void create(String path, byte[] data) throws Exception {
        zkClient.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * 获取节点数据
     */
    public String getData(String path) throws Exception {
        byte[] data = zkClient.getData(path, false, null);
        return new String(data,"UTF-8");
    }

    /**
     * 修改节点数据
     */
    public Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
        Stat stat = zkClient.setData(path, data, version);
        return stat;
    }

    /**
     * 获取节点
     */
    public void getChild(String path) throws Exception {
        List<String> list = zkClient.getChildren(path, false);
        if (!list.isEmpty()) {
            for (String child : list) {
                System.out.println("节点 = [" + child + "]");
            }
        }
    }

    /**
     * 删除节点
     */
    public void delete(final String path, int version) throws Exception{
        zkClient.delete(path, version);
    }

    public static void main(String[] args) throws Exception {
        ZkClient zk = new ZkClient();
        zk.connect();
        System.out.println("连接成功");
        zk.create("/test1", "zk".getBytes());
        String dataNode = zk.getData("/test1");
        System.out.println("/test1节点上的数据 = [" + dataNode + "]");
        zk.getChild("/");
        System.out.println("------------");
        zk.getChild("/test1");
        zk.delete("/test1", 0);
        //zk.getChild("/test1");
        zk.close();
    }
}
