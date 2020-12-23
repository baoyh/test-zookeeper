package bao.study.zookeeper.client;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;

/**
 * @author baoyh
 * @date Created in 2020/12/23 10:45
 */
public class CuratorTransaction {

    private static final String ZK_ADDRESS = "127.0.0.1:21810";
    private static final String ZK_PATH = "bao/zkTest";
    private static final String ZK_PATH_TRANSACTION = "bao/zkTransaction";

    public static void main(String[] args) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(ZK_ADDRESS, new RetryNTimes(10, 3000));
        client.start();

        String data = "hello";
        // add this to throw exception
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(ZK_PATH, data.getBytes());

        // default createMode is persistent
        String transaction = "transaction";
        client.inTransaction().
                create().withMode(CreateMode.PERSISTENT).forPath(ZK_PATH_TRANSACTION, transaction.getBytes()).
                and().
                create().withMode(CreateMode.PERSISTENT).forPath(ZK_PATH, data.getBytes()).
                and().commit();
    }
}
