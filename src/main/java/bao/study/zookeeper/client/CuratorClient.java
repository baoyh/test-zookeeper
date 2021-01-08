package bao.study.zookeeper.client;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * @author baoyh
 * @date Created in 2020/12/22 17:43
 */
public class CuratorClient {

    private static final String ZK_ADDRESS = "127.0.0.1:21810";
    private static final String ZK_PATH = "bao/zkTest";
    private static final String ZK_CHILDREN_PATH = "bao/zkTest/children";

    public static void main(String[] args) throws Exception {
        // 创建连接
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                ZK_ADDRESS,
                new RetryNTimes(10, 5000)
        );
        client.start();
        System.out.println("zk client start successfully!");

        // 创建节点 ZK_PATH, 内容为 data1
        String data1 = "hello";
        print("create", ZK_PATH, data1);
        // 判断是否已经存在节点
        Stat stat = client.checkExists().forPath(ZK_PATH);
        // 不存在则创建
        if (stat == null) {
            client.create().
                    creatingParentsIfNeeded().  // 创建路径上的父节点
                    withMode(CreateMode.EPHEMERAL). // 设置节点为临时节点, 默认为 CreateMode.PERSISTENT
                    forPath(ZK_PATH, data1.getBytes());  // 创建路径并设置
        }

        print("ls", "/");
        // 列出路径下的所有子节点
        print(client.getChildren().forPath("/"));
        print("get", ZK_PATH);
        // 获取节点下的数据
        print(client.getData().forPath(ZK_PATH));

        String data2 = "world";
        print("set", ZK_PATH, data2);
        // 重新设置路径下的值
        client.setData().forPath(ZK_PATH, data2.getBytes());
        print("get", ZK_PATH);
        print(client.getData().forPath(ZK_PATH));

        String data3 = "world";
        stat = client.checkExists().forPath(ZK_CHILDREN_PATH);
        if (stat == null) {
            print("set", ZK_CHILDREN_PATH, data3);
            // 添加子节点
            client.create().forPath(ZK_CHILDREN_PATH, data3.getBytes());
            print("get", ZK_CHILDREN_PATH);
            print(client.getData().forPath(ZK_CHILDREN_PATH));
        }

        print("delete", ZK_PATH);
        // 删除节点
        client.delete().deletingChildrenIfNeeded().forPath(ZK_PATH);
        print("ls", "/");
        print(client.getChildren().forPath("/"));


    }

    private static void print(String... commands) {
        StringBuilder text = new StringBuilder("$ ");
        for (String cmd : commands) {
            text.append(cmd).append(" ");
        }
        System.out.println(text.toString());
    }

    private static void print(Object result) {
        System.out.println(
                result instanceof byte[]
                        ? new String((byte[]) result)
                        : result);
    }
}
