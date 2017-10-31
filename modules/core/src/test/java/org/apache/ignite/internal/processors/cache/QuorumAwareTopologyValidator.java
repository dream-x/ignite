package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.UUID;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.CloseableUtils;
import org.apache.ignite.Ignite;
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

public class QuorumAwareTopologyValidator implements TopologyValidator, LifecycleAware {
    /** */
    private final int N = 10;

    /** */
    private final int RETRIES = 1000;

    /** */
    private final String SPLIT_BRAIN_ATTR = "split-brain";

    /** */
    private final String SPLIT_BRAIN_ATTR_VAL = "false";

    /** */
    private String zkConnStr;

    /** */
    static final String ACTIVATOR_NODE_ATTR = "seg.activator";

    /** */
    private transient CuratorFramework zkClient = null;

    /** */
    private String path = "/TopologiesHistory";

    /** */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /** */
    @LoggerResource
    private transient IgniteLogger log;

    /** */
    @Override public boolean validate(Collection<ClusterNode> nodes) {
        IgniteKernal kernal = (IgniteKernal)ignite;

        UUID currNodeId = ignite.cluster().localNode().id();

        ClusterNode crd = kernal.context().discovery().discoCache().oldestAliveServerNode();

        boolean resolved = F.view(nodes, new IgnitePredicate<ClusterNode>() {
            @Override public boolean apply(ClusterNode node) {
                return isMarkerNode(node);
            }
        }).size() > 0;

        if (resolved) {
            log.info("Node activator includes in the topology.");

            return true;
        }

        if (!SPLIT_BRAIN_ATTR_VAL.equals(crd.attribute(SPLIT_BRAIN_ATTR)))
            return false;

        if (checkLostPartitions(kernal, nodes))
            return false;

        if (!ignite.cluster().localNode().equals(crd))
            return true;

        if (zkClient == null)
            connect();

        long topologyVersion = kernal.cluster().topologyVersion();

        boolean checkQuorum = false;

        try {
            if (zkClient.checkExists().forPath(path) == null)
                zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path);

            String crdPath = path + "/" + currNodeId;
            byte[] crdData = new String(topologyVersion + ";" + nodes.size() + ";" +
                System.currentTimeMillis() + ";false").getBytes();

            if (zkClient.checkExists().forPath(crdPath) == null)
                zkClient.create().withMode(CreateMode.EPHEMERAL).forPath(crdPath, crdData);
            else
                zkClient.setData().forPath(crdPath, crdData);

            System.out.println("666666 " + currNodeId + " " + crd.id());

            checkQuorum = checkTopologyVersion(topologyVersion, nodes, zkClient.getChildren().forPath(path),
                currNodeId.toString(), nodes.size());
        }
        catch (Exception e) {
            log.error("Zookeeper error.", e);
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
    private boolean checkTopologyVersion(long curTopVer, Collection<ClusterNode> snapshot,
        Collection<String> histories, String nodeId, int curSize) throws Exception {
        for (String child : histories) {
            String crdPath = path + "/" + child;

            String[] params = new String(zkClient.getData().forPath(crdPath), "gbk").split(";");

            long topVer = Long.parseLong(params[0]);
            int size = Integer.parseInt(params[1]);
            long time = Long.parseLong(params[2]);
            boolean active = Boolean.parseBoolean(params[3]);

            if (histories.size() == 1) {
                setActive(true, crdPath, curTopVer, curSize);

                return true;
            }

            if (child.equals(nodeId)) {
                if (curSize < size && curTopVer > topVer) {
                    setActive(true, crdPath, curTopVer, curSize);

                    return true;
                }
            } else {
                if (curSize > size && !active)
                    setActive(true, crdPath, curTopVer, curSize);

                    return true;
            }

            setActive(false, crdPath, curTopVer, curSize);
        }

        ((IgniteKernal)ignite).context().config().setUserAttributes(F.asMap(SPLIT_BRAIN_ATTR, "true"));

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
        zkClient.setData().forPath(path,
            new String(curTopVer + ";" + curSize + ";" +
                System.currentTimeMillis() + ";" + active).getBytes());
    }
}
