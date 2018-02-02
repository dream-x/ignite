package org.apache.quorum;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CommunicationFailureContext;
import org.apache.ignite.configuration.CommunicationFailureResolver;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.CacheNameResource;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.zookeeper.CreateMode;

public class QuorumCommunicationFailureResolver implements CommunicationFailureResolver{
    //    /** */
//    private static final Logger log = LoggerFactory.getLogger(DPLTopologyValidator.class);

    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @LoggerResource
    private transient IgniteLogger log;

    /** */
    private static final int N = 10;

    /** */
    private static final int RETRIES = 1000;

    /** */
    private static final int LOCK_TIMEOUT = 60_000;

    /** */
    private static final String ACTIVATOR_NODE_ATTR = "seg.activator";

    /** */
    private static final String ZK_CONNECTION_STRING_ATTR = "zookeeper.connectionString";

    /** */
    private static final String PATH = "/Topologies";

    /** */
    private static final String SERVERS = "/Servers";

    /** */
    private static final String LOST_SERVERS = "/LostServers";

    /** */
    private transient String zkConnStr;

    /** */
    private transient static CuratorFramework zkClient;

    /** */
    @GridToStringExclude
    private final static AtomicBoolean initGuard = new AtomicBoolean();

    /** State. */
    private transient State state;

    /** */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /** */
    @CacheNameResource
    private transient String cacheName;

    @Override public void resolve(CommunicationFailureContext ctx) {
        boolean segmented = segmented(ctx.topologySnapshot());

        // segmented?
    }

    /**
     * @param node Node.
     * @return {@code True} if this is marker node.
     */
    private boolean activator(ClusterNode node) {
        return node.isClient() && node.attribute(ACTIVATOR_NODE_ATTR) != null;
    }

    /** */
    private boolean segmented(Collection<ClusterNode> nodes) {
        IgniteKernal kernal = (IgniteKernal)ignite;

        ClusterNode crd = kernal.context().discovery().discoCache().oldestAliveServerNode();

        boolean resolved = F.view(nodes, new IgnitePredicate<ClusterNode>() {
            @Override public boolean apply(ClusterNode node) {
                return activator(node);
            }
        }).size() > 0;

        if (resolved) {
            log.info("Node activator includes in the topology." + "===== " + ignite.cluster().localNode().id() + " " + nodes.size());

            return false;
        }

        if (checkLostPartitions(kernal, nodes))
            return true;

        long topologyVersion = kernal.cluster().topologyVersion();

        boolean isSplitBrain = false;

        String pathCrd = PATH + "/" + crd.id();

        try {
            if (ignite.cluster().localNode().equals(crd)) {
                log.trace("Local node eq coordinator.");
                if (zkClient.checkExists().forPath(PATH) == null) {
                    zkClient.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(PATH);
                }

                byte[] crdData;

                if (zkClient.checkExists().forPath(pathCrd) != null) {
                    log.trace("Data update coordinator. Crd=" + crd.id());

                    String[] params = new String(zkClient.getData().forPath(pathCrd), "gbk").split(";");

                    boolean active = Boolean.parseBoolean(params[3]);

                    crdData = (topologyVersion + ";" + nodes.size() + ";" +
                        System.currentTimeMillis() + ";" + active).getBytes();
                }
                else {
                    log.trace("Create new coordinator. Crd=" + crd.id());

                    if (zkClient.getChildren().forPath(PATH).size() == 0) {
                        crdData = (topologyVersion + ";" + nodes.size() + ";" +
                            System.currentTimeMillis() + ";true").getBytes();
                    } else {
                        crdData = (topologyVersion + ";" + nodes.size() + ";" +
                            System.currentTimeMillis() + ";false").getBytes();
                    }
                }

                createOrUpdate(zkClient, CreateMode.PERSISTENT, pathCrd, crdData);

                for (ClusterNode node : nodes) {
                    String pathNode = pathCrd + "/" + node.id();

                    createOrUpdate(zkClient, CreateMode.PERSISTENT, pathNode,
                        String.valueOf(topologyVersion).getBytes());
                }
            }

            isSplitBrain = !checkTopologyVersion(topologyVersion, zkClient.getChildren().forPath(PATH),
                crd.id(), nodes.size());
        }
        catch (IllegalStateException e) {
//            log.error("fix it => Crd=" + crd.id(), e);
        }
        catch (Exception e) {
            log.error("Zookeeper error. Crd=" + crd.id(), e);

            isSplitBrain = true;
        }

        if (isSplitBrain)
            log.info("Grid segmentation is detected, switching to inoperative state.");

        return isSplitBrain;
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
        InterProcessMutex lock = new InterProcessMutex(zkClient, PATH);

        int srvs = Integer.parseInt(new String(zkClient.getData().forPath(SERVERS), "gbk"));
        int lostSrvs = Integer.parseInt(new String(zkClient.getData().forPath(LOST_SERVERS), "gbk"));
        int delta = srvs - lostSrvs;

        if (ignite.cluster().localNode().id().equals(crdId)) {
            if (!lock.acquire(LOCK_TIMEOUT, TimeUnit.MILLISECONDS)) {
                log.error(crdId + " could not acquire the lock.");

                return false;
            }

            try {
                if (crds.size() == 1)
                    return true;

                for (String crd : crds) {
                    if (crdId.toString().equals(crd))
                        continue;

                    String path = PATH + "/" + crd;

                    String[] params = new String(zkClient.getData().forPath(path), "gbk").split(";");

                    long topVer = Long.parseLong(params[0]);
                    int size = Integer.parseInt(params[1]);
                    long time = Long.parseLong(params[2]);
                    boolean active = Boolean.parseBoolean(params[3]);

                    if (size > delta) {
                        if (size > curSize) {
                            log.trace("Param active=false coordinator. Crd=" + crd);

                            setActive(true, PATH + "/" + crd, topVer, size);

                            return false;
                        }

                    }
                }
            }
            catch (Exception e) {
                log.error("Error in split-brain definition.", e);
            }
            finally {
                lock.release();
            }
        }
        else {
            log.trace("Check nodes. Node=" + ignite.cluster().localNode().id());

            String path = PATH + "/" + crdId;

            String[] params = new String(zkClient.getData().forPath(path), "gbk").split(";");

            long topVer = Long.parseLong(params[0]);
            boolean active = Boolean.parseBoolean(params[3]);

            return active;
        }

        setActive(true, PATH + "/" + crdId, curTopVer, curSize);

        return true;
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

    /**
     * Sets initial validator state.
     *
     * @param nodes Topology nodes.
     */
    private void initIfNeeded(Collection<ClusterNode> nodes) {
        if (state != null)
            return;

        // Search for activator node in history on start.
        long topVer = evtNode(nodes).order();

        while (topVer > 0) {
            Collection<ClusterNode> top = ignite.cluster().topology(topVer--);

            // Stop on reaching history limit.
            if (top == null)
                return;

            boolean segmented = segmented(top);

            // Stop on reaching valid topology.
            if (!segmented)
                return;

            for (ClusterNode node : top) {
                if (activator(node)) {
                    state = State.REPAIRED;

                    return;
                }
            }
        }
    }

    /**
     * Returns node with biggest order (event topology version).
     *
     * @param nodes Topology nodes.
     * @return ClusterNode Node.
     */
    private ClusterNode evtNode(Collection<ClusterNode> nodes) {
        ClusterNode evtNode = null;

        for (ClusterNode node : nodes) {
            if (evtNode == null || node.order() > evtNode.order())
                evtNode = node;
        }

        return evtNode;
    }

    /** States. */
    private enum State {
        /** Topology is valid. */
        VALID,
        /** Topology is not valid */
        NOTVALID,
        /** Before topology will be repaired (valid) */
        BEFORE_REPARED,
        /** Topology is repaired (valid) */
        REPAIRED;
    }
}
