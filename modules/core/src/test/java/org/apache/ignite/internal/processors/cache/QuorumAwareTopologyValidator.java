package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
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
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.zookeeper.CreateMode;

public class QuorumAwareTopologyValidator implements TopologyValidator, LifecycleAware {
    /** */
    private static final int N = 10;

    /** */
    private static final int RETRIES = 1000;

    /** */
    private static final String MSG = "panic!";

    /** */
    private static final String TOPIC = "split-brain";

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
    private boolean deactivate = false;

    /** */
    @Override public boolean validate(Collection<ClusterNode> nodes) {
        IgniteKernal kernal = (IgniteKernal)ignite;

        if (!ignite.cluster().localNode().equals(kernal.context().discovery().discoCache().oldestAliveServerNode()))
            kernal.context().io().addMessageListener(TOPIC, new UserMessageListener());

        try {
            kernal.context().io().sendUserMessage(nodes, MSG, TOPIC, false, 0, true);
        }
        catch (IgniteCheckedException e) {
            e.printStackTrace();
        }

        UUID currNodeId = ignite.cluster().localNode().id();

        boolean resolved = F.view(nodes, new IgnitePredicate<ClusterNode>() {
            @Override public boolean apply(ClusterNode node) {
                return isMarkerNode(node);
            }
        }).size() > 0;

        if (resolved) {
            log.info("Node activator includes in the topology.");

            return true;
        }

        if (checkLostPartitions(kernal, nodes))
            return false;

        if (ignite.cluster().localNode().equals(kernal.context().discovery().discoCache().oldestAliveServerNode()))
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
                System.currentTimeMillis()).getBytes();

            if (zkClient.checkExists().forPath(crdPath) == null)
                zkClient.create().withMode(CreateMode.EPHEMERAL).forPath(crdPath, crdData);
            else
                zkClient.setData().forPath(crdPath, crdData);

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

        try {
            kernal.context().io().sendUserMessage(snapshot, MSG, TOPIC, false, 0, true);
        }
        catch (IgniteCheckedException e) {
            e.printStackTrace();
        }

        return false;
    }

    /** */
    private boolean checkTopologyVersion(long curTopVer, Collection<ClusterNode> snapshot,
        Collection<String> histories, String nodeId, int curSize) throws Exception {
        for (String child : histories) {
            String crdPath = path + "/" + child;

            String[] params = new String(zkClient.getData().forPath(crdPath), "gbk").split(";");

            long topVer = Long.getLong(params[0]);
            int size = Integer.getInteger(params[1]);
            long time = Long.getLong(params[2]);
            boolean active = Boolean.getBoolean(params[3]);

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

        try {
            ((IgniteKernal)ignite).context().io().sendUserMessage(snapshot, MSG, TOPIC, false, 0, true);
        }
        catch (IgniteCheckedException e) {
            e.printStackTrace();
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
        zkClient.setData().forPath(path,
            new String(curTopVer + ";" + curSize + ";" +
                System.currentTimeMillis() + ";" + active).getBytes());
    }

    private class UserMessageListener implements GridMessageListener{
        @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
            assert nodeId != null;
            assert msg != null;

            if (String.valueOf(msg).equals(MSG)) {
                System.out.println("9999" + msg);
                deactivate = true;
            }
        }
    };
}
