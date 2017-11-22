package org.apache.ignite.internal.processors.cache;

import java.util.List;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.utils.CloseableUtils;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

public class QuorumAwareTopologyValidatorSplitTest extends GridCommonAbstractTest {
    /** */
    private static final String ZOOKEEPER_CONNECTIONS_STR = "0.0.0.0:2181,0.0.0.0:2181,0.0.0.0:2181";

    /** */
    private static final int GRID_CNT = 10;

    /** */
    private static final int CACHES_CNT = 1;

    /** */
    private static final int RESOLVER_GRID_IDX = GRID_CNT;

    /** */
    private static final int CONFIGLESS_GRID_IDX = GRID_CNT + 1;

    /** */
    private static final int ZK_CLUSTER_SIZE = 3;

    /** */
    private TestingCluster zkCluster;

    @Override public void beforeTest() throws Exception {
        super.beforeTest();

        // start the ZK cluster
//        zkCluster = new TestingCluster(ZK_CLUSTER_SIZE);
//        zkCluster.start();

         System.setProperty("zookeeper.connectionString", ZOOKEEPER_CONNECTIONS_STR);

        startGridsMultiThreaded(GRID_CNT);
    }

    /**
     * After test.
     *
     * @throws Exception
     */
    @Override public void afterTest() throws Exception {
        super.afterTest();

        if (zkCluster != null)
            CloseableUtils.closeQuietly(zkCluster);

        stopAllGrids();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        int idx = getTestIgniteInstanceIndex(gridName);

        if (idx != CONFIGLESS_GRID_IDX) {
            if (idx == RESOLVER_GRID_IDX) {
                cfg.setClientMode(true);

                cfg.setUserAttributes(F.asMap("seg.activator", "true"));
            } else {
                CacheConfiguration[] ccfgs = new CacheConfiguration[CACHES_CNT];

                for (int cnt = 0; cnt < CACHES_CNT; cnt++) {
                    CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

                    ccfg.setName(testCacheName(cnt));
                    ccfg.setCacheMode(PARTITIONED);
                    ccfg.setBackups(0);
//                    ccfg.setTopologyValidator(new QuorumAwareTopologyValidator());
                    ccfg.setTopologyValidator(new SimpleAwareTopologyValidator());
//                    ccfg.setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE);

                    ccfgs[cnt] = ccfg;
                }

                cfg.setUserAttributes(F.asMap("split-brain", "false"));

                cfg.setCacheConfiguration(ccfgs);
            }
        }

        return cfg;
    }

    /**
     * @param idx Index.
     */
    private String testCacheName(int idx) {
        return "test" + idx;
    }

    /**
     * Tests topology split scenario.
     *
     * @throws Exception If failed.
     */
    public void testTopologyValidator() throws Exception {
        // Tests each node is able to do puts.
        tryPut(0, 1, 2, 3, 4, 5);

        clearAll();

        stopGrid(1, true);
        stopGrid(0, true);
        stopGrid(3, true);
        stopGrid(7, true);
        stopGrid(2, true);
        stopGrid(4, true);

        awaitPartitionMapExchange();

        tryPut(0, 2, 4, 5);

        clearAll();

        startGrid(1);
        startGrid(3);
        stopGrid(2);
        stopGrid(4);

        awaitPartitionMapExchange();

        try {
            tryPut(0, 1, 3, 5);

            fail();
        } catch (Exception e) {
            // No-op.
        }

        startGrid(RESOLVER_GRID_IDX);

        tryPut(0, 1, 3, 5);

        clearAll();

        startGrid(CONFIGLESS_GRID_IDX);

        awaitPartitionMapExchange();

        tryPut(CONFIGLESS_GRID_IDX);

        stopGrid(CONFIGLESS_GRID_IDX);

        stopGrid(RESOLVER_GRID_IDX);

        awaitPartitionMapExchange();

        try {
            tryPut(0, 1, 3, 5);

            fail();
        } catch (Exception e) {
            // No-op.
        }

        startGrid(RESOLVER_GRID_IDX);

        stopGrid(RESOLVER_GRID_IDX);

        clearAll();

        startGrid(2);
        startGrid(4);

        awaitPartitionMapExchange();

        tryPut(0, 1, 2, 3, 4, 5);

        stopGrid(1);
        stopGrid(3);
        stopGrid(5);

        awaitPartitionMapExchange();
    }

    /** */
    private void clearAll() {
        for (int i = 0; i < CACHES_CNT; i++)
            grid(0).cache(testCacheName(i)).clear();
    }

    /**
     * @param grids Grids to test.
     */
    private void tryPut(int... grids) {
        for (int i = 0; i < grids.length; i++) {
            IgniteEx g = grid(grids[i]);

            for (int cnt = 0; cnt < CACHES_CNT; cnt++) {
                String cacheName = testCacheName(cnt);

                for (int k = 0; k < 200; k++) {
                    if (g.affinity(cacheName).isPrimary(g.localNode(), k)) {
                        log().info("Put " + k + " to node " + g.localNode().id().toString());

                        IgniteCache<Object, Object> cache = g.cache(cacheName);

                        cache.put(k, k);

                        assertEquals(1, cache.localSize());

                        break;
                    }
                }
            }
        }
    }

    private void printAttr() {
        System.out.println(grid(0).cluster().nodes().size());

        for (ClusterNode node : grid(0).cluster().nodes()) {
            System.out.println((String)node.attribute(MainDCMajorityAwareTopologyValidator.DC_NODE_ATTR));
        }
    }
}
