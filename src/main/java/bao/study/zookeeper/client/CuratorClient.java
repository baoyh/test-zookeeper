package bao.study.zookeeper.client;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/**
 * @author baoyh
 * @date Created in 2020/12/22 17:43
 */
public class CuratorClient {

    /**
     * Zookeeper info
     */
    private static final String ZK_ADDRESS = "127.0.0.1:21810";
    private static final String ZK_PATH = "bao/zkTest";
    private static final String ZK_CHILDREN_PATH = "bao/zkTest/children";

    public static void main(String[] args) throws Exception {
        // 1.Connect to zk
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                ZK_ADDRESS,
                new RetryNTimes(10, 5000)
        );
        client.start();
        System.out.println("zk client start successfully!");

        // 2.Client API test
        // 2.1 Create node
        String data1 = "hello";
        print("create", ZK_PATH, data1);
        Stat stat = client.checkExists().forPath(ZK_PATH);
        if (stat == null) {
            client.create().
                    creatingParentsIfNeeded().
                    withMode(CreateMode.EPHEMERAL).
                    forPath(ZK_PATH, data1.getBytes());
        }

        // 2.2 Get node and data
        print("ls", "/");
        print(client.getChildren().forPath("/"));
        print("get", ZK_PATH);
        print(client.getData().forPath(ZK_PATH));

        // 2.3 Modify data
        String data2 = "world";
        print("set", ZK_PATH, data2);
        client.setData().forPath(ZK_PATH, data2.getBytes());
        print("get", ZK_PATH);
        print(client.getData().forPath(ZK_PATH));

        // 2.4 Add child path
        String data3 = "world";
        stat = client.checkExists().forPath(ZK_CHILDREN_PATH);
        if (stat == null) {
            print("set", ZK_CHILDREN_PATH, data3);
            client.create().forPath(ZK_CHILDREN_PATH, data3.getBytes());
            print("get", ZK_CHILDREN_PATH);
            print(client.getData().forPath(ZK_CHILDREN_PATH));
        }

        // 2.5 Remove node
        print("delete", ZK_PATH);
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
