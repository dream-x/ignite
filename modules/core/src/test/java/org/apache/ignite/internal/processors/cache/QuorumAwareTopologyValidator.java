/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
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
import org.apache.ignite.resources.CacheNameResource;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.zookeeper.CreateMode;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class QuorumAwareTopologyValidator implements TopologyValidator, LifecycleAware {
//    /** */
//    private static final Logger log = LoggerFactory.getLogger(DPLTopologyValidator.class);

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
    private static final String ACTIVATOR_NODE_ATTR = "split.resolved";

    /** */
    private static final String PATH = "/Topologies";

    /** */
    private static final String SERVERS = "/Servers";

    /** */
    private static final String LOST_SERVERS = "/LostServers";

    /** */
    private transient volatile String zkConnStr;

    /** */
    private transient volatile CuratorFramework zkClient = null;

    /** State. */
    private transient State state;

    /** */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /** */
    @CacheNameResource
    private transient String cacheName;

    /** */
    @Override public boolean validate(Collection<ClusterNode> nodes) {
        initIfNeeded(nodes);

//        for (ClusterNode node : F.view(nodes, new IgnitePredicate<ClusterNode>() {
//            @Override public boolean apply(ClusterNode node) {
//                return !node.isClient() && node.attribute(DC_NODE_ATTR) == null;
//            }
//        })) {
//            log.error("Not valid server nodes are detected in topology: [cacheName=" + cacheName + ", node=" +
//                node + ']');
//
//            return false;
//        }

        boolean segmented = segmented(nodes);

        if (!segmented)
            state = State.VALID; // Also clears possible BEFORE_REPAIRED and REPAIRED states.
        else {
            if (state == State.REPAIRED) // Any topology change in segmented grid in repaired mode is valid.
                return true;

            // Find discovery event node.
            ClusterNode evtNode = evtNode(nodes);

            if (activator(evtNode))
                state = State.BEFORE_REPARED;
            else {
                if (state == State.BEFORE_REPARED) {
                    boolean activatorLeft = true;

                    // Check if activator is no longer in topology.
                    for (ClusterNode node : nodes) {
                        if (node.isClient() && activator(node)) {
                            activatorLeft = false;

                            break;
                        }
                    }

                    if (activatorLeft) {
                        if (log.isInfoEnabled())
                            log.info("Grid segmentation is repaired: [cacheName=" + cacheName + ']');

                        state = State.REPAIRED; // Switch to REPAIRED state only when activator leaves.
                    } // Else stay in BEFORE_REPARED state.
                }
                else {
                    if (state == State.VALID) {
                        if (log.isInfoEnabled())
                            log.info("Grid segmentation is detected: [cacheName=" + cacheName + ']');
                    }

                    state = State.NOTVALID;
                }
            }
        }

        return state == State.VALID || state == State.REPAIRED;
    }

    /** */
    @Override public void start() throws IgniteException {
        zkConnStr = System.getProperty("zookeeper.connectionString");
    }

    /** */
    @Override public void stop() throws IgniteException {
        CloseableUtils.closeQuietly(zkClient);
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
                return isMarkerNode(node);
            }
        }).size() > 0;

        if (resolved) {
            log.info("Node activator includes in the topology.");

            return true;
        }

        if (checkLostPartitions(kernal, nodes))
            return false;

        if (zkClient == null)
            connect();

        long topologyVersion = kernal.cluster().topologyVersion();

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
            }

            checkQuorum = checkTopologyVersion(topologyVersion, zkClient.getChildren().forPath(PATH),
                crd.id(), nodes.size());
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

        Map<String, Map<String, Integer>> topologies = new HashMap<>();

        int srvs = Integer.parseInt(new String(zkClient.getData().forPath(SERVERS), "gbk"));
        int lostSrvs = Integer.parseInt(new String(zkClient.getData().forPath(LOST_SERVERS), "gbk"));
        int delta = srvs - lostSrvs;

        // если ты координатор то смотришь текущую топологию и валидируешь по списску
        // мин количество серверов
        // проверяет мощность других координаторов (кластеров) и сравниет со своей
        // обновляет статус активный или нет и возвращает такой же ответ для TV
        if (!lock.acquire(LOCK_TIMEOUT, TimeUnit.MILLISECONDS)) {
            log.error(crdId + " could not acquire the lock.");

            return false;
        }

        try {
            if (ignite.cluster().localNode().id().equals(crdId)) {
                for (String crd : crds) {
                    if (crdId.toString().equals(crd))
                        continue;

                    String path = PATH + "/" + crd;

                    String[] params = new String(zkClient.getData().forPath(path), "gbk").split(";");

                    long topVer = Long.parseLong(params[0]);
                    int size = Integer.parseInt(params[1]);
                    long time = Long.parseLong(params[2]);
                    boolean active = Boolean.parseBoolean(params[3]);

                    if (size > curSize && size > delta) {
                        setActive(false, PATH + "/" + crdId, curTopVer, curSize);

                        return false;
                    }
                }
            } else {
                String path = PATH + "/" + crdId;

                String[] params = new String(zkClient.getData().forPath(path), "gbk").split(";");

                return Boolean.parseBoolean(params[3]);
            }
        } catch (Exception e) {
            log.error("Error in split-brain definition.", e);
        } finally {
            lock.release();
        }

        setActive(false, PATH + "/" + crdId, curTopVer, curSize);

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

    private class Cluster {
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

        public String getId() {
            return id;
        }

        public long getTopVer() {
            return topVer;
        }

        public boolean isActive() {
            return active;
        }
    }
}
