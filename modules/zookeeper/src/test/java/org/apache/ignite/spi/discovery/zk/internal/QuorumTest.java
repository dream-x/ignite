package org.apache.ignite.spi.discovery.zk.internal;

import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.apache.curator.test.TestingCluster;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CommunicationFailureResolver;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.discovery.DiscoveryLocalJoinData;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpiInternalListener;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiNodeAuthenticator;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.zookeeper.ZkTestClientCnxnSocketNIO;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SECURITY_CREDENTIALS;
import static org.apache.zookeeper.ZooKeeper.ZOOKEEPER_CLIENT_CNXN_SOCKET;

public class QuorumTest extends GridCommonAbstractTest {
    /** */
    private static final String IGNITE_ZK_ROOT = ZookeeperDiscoverySpi.DFLT_ROOT_PATH;

    /** */
    private static final int ZK_SRVS = 3;

    /** */
    private static TestingCluster zkCluster;

    /** To run test with real local ZK. */
    private static final boolean USE_TEST_CLUSTER = true;

    /** */
    private boolean client;

    /** */
    private static ThreadLocal<Boolean> clientThreadLoc = new ThreadLocal<>();

    /** */
    private static ConcurrentHashMap<UUID, Map<Long, DiscoveryEvent>> evts = new ConcurrentHashMap<>();

    /** */
    private static volatile boolean err;

    /** */
    private boolean testSockNio;

    /** */
    private boolean testCommSpi;

    /** */
    private long sesTimeout;

    /** */
    private long joinTimeout;

    /** */
    private boolean clientReconnectDisabled;

    /** */
    private ConcurrentHashMap<String, ZookeeperDiscoverySpi> spis = new ConcurrentHashMap<>();

    /** */
    private Map<String, Object> userAttrs;

    /** */
    private boolean dfltConsistenId;

    /** */
    private UUID nodeId;

    /** */
    private boolean persistence;

    /** */
    private IgniteOutClosure<CommunicationFailureResolver> commFailureRslvr;

    /** */
    private IgniteOutClosure<DiscoverySpiNodeAuthenticator> auth;

    /** */
    private String zkRootPath;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String igniteInstanceName) throws Exception {
        if (testSockNio)
            System.setProperty(ZOOKEEPER_CLIENT_CNXN_SOCKET, ZkTestClientCnxnSocketNIO.class.getName());

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (nodeId != null)
            cfg.setNodeId(nodeId);

        if (!dfltConsistenId)
            cfg.setConsistentId(igniteInstanceName);

        ZookeeperDiscoverySpi zkSpi = new ZookeeperDiscoverySpi();

        if (joinTimeout != 0)
            zkSpi.setJoinTimeout(joinTimeout);

        zkSpi.setSessionTimeout(sesTimeout > 0 ? sesTimeout : 10_000);

        zkSpi.setClientReconnectDisabled(clientReconnectDisabled);

        // Set authenticator for basic sanity tests.
        if (auth != null) {
            zkSpi.setAuthenticator(auth.apply());

            zkSpi.setInternalListener(new IgniteDiscoverySpiInternalListener() {
                @Override public void beforeJoin(ClusterNode locNode, IgniteLogger log) {
                    ZookeeperClusterNode locNode0 = (ZookeeperClusterNode)locNode;

                    Map<String, Object> attrs = new HashMap<>(locNode0.getAttributes());

                    attrs.put(ATTR_SECURITY_CREDENTIALS, new SecurityCredentials(null, null, igniteInstanceName));

                    locNode0.setAttributes(attrs);
                }

                @Override public boolean beforeSendCustomEvent(DiscoverySpi spi, IgniteLogger log, DiscoverySpiCustomMessage msg) {
                    return false;
                }
            });
        }

        spis.put(igniteInstanceName, zkSpi);

        if (USE_TEST_CLUSTER) {
            assert zkCluster != null;

            zkSpi.setZkConnectionString(zkCluster.getConnectString());

            if (zkRootPath != null)
                zkSpi.setZkRootPath(zkRootPath);
        }
        else
            zkSpi.setZkConnectionString("localhost:2181");

        cfg.setDiscoverySpi(zkSpi);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        Boolean clientMode = clientThreadLoc.get();

        if (clientMode != null)
            cfg.setClientMode(clientMode);
        else
            cfg.setClientMode(client);

        if (userAttrs != null)
            cfg.setUserAttributes(userAttrs);

        Map<IgnitePredicate<? extends Event>, int[]> lsnrs = new HashMap<>();

        lsnrs.put(new IgnitePredicate<Event>() {
            /** */
            @IgniteInstanceResource
            private Ignite ignite;

            @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
            @Override public boolean apply(Event evt) {
                try {
                    DiscoveryEvent discoveryEvt = (DiscoveryEvent)evt;

                    UUID locId = ((IgniteKernal)ignite).context().localNodeId();

                    Map<Long, DiscoveryEvent> nodeEvts = evts.get(locId);

                    if (nodeEvts == null) {
                        Object old = evts.put(locId, nodeEvts = new TreeMap<>());

                        assertNull(old);

                        synchronized (nodeEvts) {
                            DiscoveryLocalJoinData locJoin = ((IgniteKernal)ignite).context().discovery().localJoin();

                            nodeEvts.put(locJoin.event().topologyVersion(), locJoin.event());
                        }
                    }

                    synchronized (nodeEvts) {
                        DiscoveryEvent old = nodeEvts.put(discoveryEvt.topologyVersion(), discoveryEvt);

                        assertNull(old);
                    }
                }
                catch (Throwable e) {
                    error("Unexpected error [evt=" + evt + ", err=" + e + ']', e);

                    err = true;
                }

                return true;
            }
        }, new int[]{EVT_NODE_JOINED, EVT_NODE_FAILED, EVT_NODE_LEFT});

        cfg.setLocalEventListeners(lsnrs);

        if (persistence) {
            DataStorageConfiguration memCfg = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setMaxSize(100 * 1024 * 1024).
                    setPersistenceEnabled(true))
                .setPageSize(1024)
                .setWalMode(WALMode.LOG_ONLY);

            cfg.setDataStorageConfiguration(memCfg);
        }

        if (testCommSpi)
            cfg.setCommunicationSpi(new ZkTestCommunicationSpi());

        if (commFailureRslvr != null)
            cfg.setCommunicationFailureResolver(commFailureRslvr.apply());

        return cfg;
    }

    /**
     *
     */
    public void testLostNodesCluster() throws Exception {
        startGridsMultiThreaded(5, 5);

        waitForTopology(10);
    }

    /**
     *
     */
    public void testSplitBrainCluster() throws Exception {
        startGridsMultiThreaded(5, 5);

        waitForTopology(10);
    }

    /**
     *
     */
    public void testLostPartitionsCluster() throws Exception {
        /* No-op */
    }

    /**
     *
     */
    public void testSplitBrainMixCluster() throws Exception {
        /* No-op */
    }

    /**
     *
     */
    public void testSplitBrainMixAndLostPartitionsCluster() throws Exception {
        /* No-op */
    }

    /**
     *
     */
    public void testLostHalfCluster() throws Exception {
        /* No-op */
    }

    /**
     *
     */
    public void testOffHalfCluster() throws Exception {
        /* No-op */
    }

    /**
     *
     */
    public void testBreakOffZk() throws Exception {
        /* No-op */
    }

    /**
     *
     */
    static class ZkTestCommunicationSpi extends TestRecordingCommunicationSpi {
        /** */
        private volatile CountDownLatch pingStartLatch;

        /** */
        private volatile CountDownLatch pingLatch;

        /** */
        private volatile BitSet checkRes;

        /**
         * @param ignite Node.
         * @return Node's communication SPI.
         */
        static ZkTestCommunicationSpi testSpi(Ignite ignite) {
            return (ZkTestCommunicationSpi)ignite.configuration().getCommunicationSpi();
        }

        /**
         * @param nodes Number of nodes.
         * @param setBitIdxs Bits indexes to set in check result.
         */
        void initCheckResult(int nodes, Integer... setBitIdxs) {
            checkRes = new BitSet(nodes);

            for (Integer bitIdx : setBitIdxs)
                checkRes.set(bitIdx);
        }

        /** {@inheritDoc} */
        @Override public IgniteFuture<BitSet> checkConnection(List<ClusterNode> nodes) {
            CountDownLatch pingStartLatch = this.pingStartLatch;

            if (pingStartLatch != null)
                pingStartLatch.countDown();

            CountDownLatch pingLatch = this.pingLatch;

            try {
                if (pingLatch != null)
                    pingLatch.await();
            }
            catch (InterruptedException e) {
                throw new IgniteException(e);
            }

            BitSet checkRes = this.checkRes;

            if (checkRes != null) {
                this.checkRes = null;

                return new IgniteFinishedFutureImpl<>(checkRes);
            }

            return super.checkConnection(nodes);
        }
    }
}
