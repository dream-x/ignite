package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.CloseableUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.zookeeper.CreateMode;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
@SuppressWarnings("unused")
public class QuorumAwareTopologyValidator implements TopologyValidator, LifecycleAware {
    /** */
//    private static final Logger log = LoggerFactory.getLogger(DPLTopologyValidator.class);

    /** */
    private static final int N = 10;

    /** */
    private static final int RETRIES = 1000;

    /** */
    private static final String ACTIVATOR_NODE_ATTR = "seg.activator";

    /** */
    private static final String PATH = "/Topologies";

    /** */
    private static final String COUNT_SERVERS = "/CountServers";

    /** */
    private static final String LOST_SERVERS = "/CountLostServers";

    /** */
    private transient volatile String zkConnStr;

    /** */
    private transient volatile CuratorFramework zkClient = null;

    /** */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /** */
    @LoggerResource
    private transient IgniteLogger log;

    /** */
    @Override public boolean validate(Collection<ClusterNode> nodes) {
        IgniteKernal kernal = (IgniteKernal)ignite;

        ClusterNode crd = kernal.context().discovery().discoCache().oldestAliveServerNode();

        boolean resolved = F.view(nodes, new IgnitePredicate<ClusterNode>() {
            @Override public boolean apply(ClusterNode node) {
                return isMarkerNode(node);
            }
        }).size() > 0;

        System.out.println("++++++++++ " + ignite.cluster().localNode().id()+ " coord " + crd.id() + " time " + System.currentTimeMillis());

        if (resolved) {
            log.info("Node activator includes in the topology.");

            return true;
        }

        if (checkLostPartitions(kernal, nodes))
            return false;

        if (zkClient == null)
            connect();

        long topologyVersion = kernal.cluster().topologyVersion();

        long topologyVersion2 = kernal.context().discovery().topologyVersion();

        System.out.println("==== 1: " + topologyVersion + "  " + "2: " + topologyVersion2 + " n: " + nodes.size());

        boolean checkQuorum = false;

        String pathCrd = PATH + "/" + crd.id();

        try {
            if (ignite.cluster().localNode().equals(crd)) {
                if (zkClient.checkExists().forPath(PATH) == null) {
                    zkClient.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(PATH);
                }

                byte[] crdData = (topologyVersion + ";" + nodes.size() + ";" +
                    System.currentTimeMillis() + ";false").getBytes();

                createOrUpdate(zkClient, CreateMode.PERSISTENT, pathCrd, crdData);

                for (ClusterNode node : nodes) {
                    String pathNode = pathCrd + "/" + node.id();

                    createOrUpdate(zkClient, CreateMode.PERSISTENT, pathNode,
                        String.valueOf(topologyVersion).getBytes());
                }
            } else {
                if (zkClient.checkExists().forPath(pathCrd) == null) {
                    log.error("Not fount entry in ZooKeeper.");

                    return false;
                }

                System.out.println("For node: " + ignite.cluster().localNode().id() + " coord: " + crd);
            }

//            checkQuorum = checkTopologyVersion(topologyVersion, zkClient.getChildren().forPath(PATH),
//                crd.id(), nodes.size());

            checkQuorum = true;
        }
        catch (Exception e) {
            log.error("Zookeeper error.", e);

            checkQuorum = false;
        }

        if (!checkQuorum)
            log.info("Grid segmentation is detected, switching to inoperative state.");

        return checkQuorum;
    }

    /** */
    @Override public void start() throws IgniteException {
        zkConnStr = System.getProperty("zookeeper.connectionString");
    }

    /** */
    @Override public void stop() throws IgniteException {
        CloseableUtils.closeQuietly(zkClient);
    }

    /** */
    private boolean checkLostPartitions(IgniteKernal kernal, Collection<ClusterNode> snapshot) {
        boolean partitionLost = false;

        for (CacheGroupContext grp : kernal.context().cache().cacheGroups()) {
            if (grp.isLocal())
                continue;

            if (!grp.topology().lostPartitions().isEmpty()) {
                partitionLost = true;
                break;
            }
        }

        if (partitionLost) {
            log.info("Grid partition lost is detected, switching to inoperative state.");

            return true;
        }

        return false;
    }

    /** */
    private boolean checkTopologyVersion(long curTopVer, Collection<String> crds,
        UUID crdId, int curSize) throws Exception {

        Map<String, Map<String, Integer>> topologies = new HashMap<>();

        int cntSrv = Integer.parseInt(new String(zkClient.getData().forPath(COUNT_SERVERS), "gbk"));
        int cntLostSrv = Integer.parseInt(new String(zkClient.getData().forPath(LOST_SERVERS), "gbk"));
        int delta = cntSrv - cntLostSrv;

        // если ты координатор то смотришь текущую топологию и валидируешь по списску
        // мин количество серверов
        // проверяет мощность других координаторов (кластеров)
        // обновляет статус активный или нет и возвращает такой же ответ для TV
        if (ignite.cluster().localNode().id().equals(crdId)) {
            for (String crd : crds) {
                String path = PATH + "/" + crd;

//                String[] params = new String(zkClient.getData().forPath(path), "gbk").split(";");

//                long topVer = Long.parseLong(params[0]);
//                int size = Integer.parseInt(params[1]);
//                long time = Long.parseLong(params[2]);
//                boolean active = Boolean.parseBoolean(params[3]);

                Collection<String> nodesByCrd = zkClient.getChildren().forPath(path);

                topologies.put(crd, new HashMap<>());

                for (String node : nodesByCrd) {
                    topologies.get(crd).put(node, Integer.parseInt(new String(
                        zkClient.getData().forPath(path + "/" + node), "gbk")));
                }


            }
        } else {
            String path = PATH + "/" + crdId;

            String[] params = new String(zkClient.getData().forPath(path), "gbk").split(";");

            boolean active = Boolean.parseBoolean(params[3]);

            return active;
        }

        return false;
    }

    /** */
    private void connect() {
        zkClient = CuratorFrameworkFactory.newClient(zkConnStr, new RetryNTimes(N, RETRIES));
        zkClient.start();
    }

    /**
     * @param node Node.
     * @return {@code True} if this is marker node.
     */
    private boolean isMarkerNode(ClusterNode node) {
        return node.isClient() && node.attribute(ACTIVATOR_NODE_ATTR) != null;
    }

    /** */
    private void setActive(boolean active, String path, long curTopVer, int curSize) throws Exception {
        zkClient.setData().forPath(path, (curTopVer + ";" + curSize + ";" +
            System.currentTimeMillis() + ";" + active).getBytes());
    }

    /** */
    private void createOrUpdate(CuratorFramework zkClient, CreateMode mode, String path, byte[] data) throws Exception {
        if (zkClient.checkExists().forPath(path) == null)
            zkClient.create().withMode(mode).forPath(path, data);
        else
            zkClient.setData().forPath(path, data);
    }

    private class Cluster implements Comparable<Cluster> {
        private String id;
        private int size;
        private long topVer;
        private boolean active;
        private long time;

        public Cluster(String id, int size, long topVer, boolean active, long time) {
            this.id = id;
            this.size = size;
            this.topVer = topVer;
            this.active = active;
            this.time = time;
        }

        public int getSize() {
            return size;
        }

        public long getTime() {
            return time;
        }

        @Override public int compareTo(@NotNull Cluster cluster) {
            // добавить более сложную сортировку
            return time > cluster.getTime() ? 1 : time < cluster.getTime() ? -1 : 0;
        }
    }
}
