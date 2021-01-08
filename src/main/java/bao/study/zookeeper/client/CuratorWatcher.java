package bao.study.zookeeper.client;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.cache.*;
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

        // -----------  一次注册一次监听  ------------------
        client.getData().usingWatcher((org.apache.curator.framework.api.CuratorWatcher) event -> {
            System.out.println("[CuratorWatcher] Type: " + event.getType() );
        }).forPath(ZK_PATH);


        // -----------  一次注册多次监听  ------------------
        // 仅监听当前节点的变化
        NodeCache nodeCache = new NodeCache(client, ZK_PATH, true);
        // 添加监听器
        nodeCache.getListenable().addListener(() -> System.out.println("[NodeCache] New data: " + new String(nodeCache.getCurrentData().getData())));
        // 启动监听
        nodeCache.start();

        // 仅监听当前节点和子节点的变化
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, ZK_PATH, true);
        pathChildrenCache.getListenable().addListener((zkClient, event) -> {
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
        Listenable<TreeCacheListener> treeCacheListenable = treeCache.getListenable();
        treeCacheListenable.addListener((zkClient, event) -> {
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
