package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.UUID;
import java.util.logging.Logger;
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
import org.slf4j.LoggerFactory;

/**
 *
 */
@SuppressWarnings("unused")
public class QuorumAwareTopologyValidator implements TopologyValidator, LifecycleAware {
    /** */
//    private static final Logger LOGGER = LoggerFactory.getLogger(DPLTopologyValidator.class);

    /** */
    private static final int N = 10;

    /** */
    private static final int RETRIES = 1000;

    /** */
    private static final String SPLIT_BRAIN_ATTR = "split-brain";

    /** */
    private static final String SPLIT_BRAIN_ATTR_VAL = "false";

    /** */
    private static final String ACTIVATOR_NODE_ATTR = "seg.activator";

    /** */
    private static final String PATH = "/TopologiesHistory";

    /** */
    private transient volatile String zkConnStr;

    /** */
    private transient volatile CuratorFramework zkClient = null;

    /** */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /** */
    @LoggerResource
    private transient IgniteLogger LOGGER;

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
            ((IgniteKernal)ignite).context().config().setUserAttributes(F.asMap(SPLIT_BRAIN_ATTR, SPLIT_BRAIN_ATTR_VAL));

            LOGGER.info("Node activator includes in the topology.");

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
            if (zkClient.checkExists().forPath(PATH) == null)
                zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(PATH);

            String crdPath = PATH + "/" + currNodeId;
            byte[] crdData = new String(topologyVersion + ";" + nodes.size() + ";" +
                System.currentTimeMillis() + ";false").getBytes();

            if (zkClient.checkExists().forPath(crdPath) == null)
                zkClient.create().withMode(CreateMode.EPHEMERAL).forPath(crdPath, crdData);
            else
                zkClient.setData().forPath(crdPath, crdData);

            checkQuorum = checkTopologyVersion(topologyVersion, nodes, zkClient.getChildren().forPath(PATH),
                currNodeId.toString(), nodes.size());
        }
        catch (Exception e) {
            LOGGER.error("Zookeeper error.", e);
        }

        if (!checkQuorum)
            LOGGER.info("Grid segmentation is detected, switching to inoperative state.");

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
            LOGGER.info("Grid partition lost is detected, switching to inoperative state.");

            return true;
        }

        return false;
    }

    /** */
    private boolean checkTopologyVersion(long curTopVer, Collection<ClusterNode> snapshot,
        Collection<String> histories, String nodeId, int curSize) throws Exception {
        for (String child : histories) {
            String crdPath = PATH + "/" + child;

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
                if (curSize < size && curTopVer > topVer && active) {
                    setActive(true, crdPath, curTopVer, curSize);

                    return true;
                }
            } else {
                if (active) {
                    ((IgniteKernal)ignite).context().config().setUserAttributes(F.asMap(SPLIT_BRAIN_ATTR, "true"));

                    return false;
                }

                if (curSize > size && !active) {
                    setActive(true, crdPath, curTopVer, curSize);

                    return true;
                }
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
