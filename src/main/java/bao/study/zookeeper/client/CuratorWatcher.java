package bao.study.zookeeper.client;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.retry.RetryNTimes;

/**
 * @author baoyh
 * @date Created in 2020/12/22 18:02
 */
public class CuratorWatcher {

    /**
     * Zookeeper info
     */
    private static final String ZK_ADDRESS = "127.0.0.1:21810";
    private static final String ZK_PATH = "bao/zkTest";

    public static void main(String[] args) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                ZK_ADDRESS,
                new RetryNTimes(10, 5000)
        );
        client.start();
        System.out.println("zk client start successfully!");

        // 仅监听当前节点的变化
        NodeCache nodeCache = new NodeCache(client, ZK_PATH, true);
        nodeCache.getListenable().addListener(() -> System.out.println("[NodeCache] New data: " + new String(nodeCache.getCurrentData().getData())));
        nodeCache.start();

        // 监听当前节点和子节点的变化
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, ZK_PATH, true);
        pathChildrenCache.getListenable().addListener((zkClient, event) -> {
            // Listener for PathChildrenCache changes
            ChildData data = event.getData();
            if (data == null) {
                System.out.println("[PathChildrenCache] No data in event[" + event + "]");
            } else {
                System.out.println("[PathChildrenCache] Receive event: "
                        + "type=[" + event.getType() + "]"
                        + ", path=[" + data.getPath() + "]"
                        + ", data=[" + new String(data.getData()) + "]"
                        + ", stat=[" + data.getStat() + "]");
            }
        });
        pathChildrenCache.start();

        // 监听当前节点和改节点下所有节点的变化
        TreeCache treeCache = new TreeCache(client, ZK_PATH);
        treeCache.getListenable().addListener((zkClient, event) -> {
            ChildData data = event.getData();
            if (data == null) {
                System.out.println("[TreeCache] No data in event[" + event + "]");
            } else {
                System.out.println("[TreeCache] Receive event: "
                        + "type=[" + event.getType() + "]"
                        + ", path=[" + data.getPath() + "]"
                        + ", data=[" + new String(data.getData()) + "]"
                        + ", stat=[" + data.getStat() + "]");
            }
        });
        treeCache.start();
    }
}
