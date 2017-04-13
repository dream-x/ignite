package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

public class TxSavepointsSelfTest extends GridCommonAbstractTest {

    private IgniteCache<Integer, Integer> cache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disc = new TcpDiscoverySpi();

        disc.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(disc);

        cfg.setCacheConfiguration(new CacheConfiguration().setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override
    protected void beforeTest() throws Exception {
        super.beforeTest();
        startGrid(0);
        cache = grid(0).cache(null);
    }

    /** {@inheritDoc} */
    @Override
    protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     *
     */
    public void testMultipleSavepoints() {
        try (Transaction tx = grid(0).transactions().txStart()) {
            putThreeValuesAndCreateSavepoints(tx);
            tx.rollbackToSavepoint("s2");
            tx.commit();
        }
        assertEquals((Integer) 2, cache.get(2));
    }

    /**
     * Tests valid and invalid rollbacks to savepoint.
     */
    public void testMultipleRollbacksToSavepoint() {
        try (Transaction tx = grid(0).transactions().txStart()) {
            putThreeValuesAndCreateSavepoints(tx);
            tx.rollbackToSavepoint("s2");
            assertEquals((Integer) 2, cache.get(2));
            tx.rollbackToSavepoint("s2");
            assertEquals((Integer) 2, cache.get(2));
            putThreeValuesAndCreateSavepoints(tx);
            tx.rollbackToSavepoint("s3");
        } catch (Exception e) {
            assertTrue("Unexpected exception: " + e.getMessage(), e.getMessage().startsWith("No such savepoint."));
        }
    }

    /**
     * Tests savepoint deleting.
     */
    public void testReleaseSavepoints() {
        try (Transaction tx = grid(0).transactions().txStart()) {
            putThreeValuesAndCreateSavepoints(tx);
            tx.releaseCheckpoint("s1");
            tx.releaseCheckpoint("s3");
            tx.rollbackToSavepoint("s2");
            tx.commit();
        }
        assertEquals((Integer) 2, cache.get(2));
    }

    /**
     * Tests rollbacks to the same savepoint instance.
     */
    public void testDoubleRollbackToSavepoint() {
        try (Transaction tx = grid(0).transactions().txStart()) {
            putThreeValuesAndCreateSavepoints(tx);
            tx.rollbackToSavepoint("s2");
            cache.put(2, 3);
            tx.rollbackToSavepoint("s2");
            tx.commit();
        }
        assertEquals((Integer) 2, cache.get(2));
    }

    /**
     * Tests savepoints in failed transaction.
     */
    public void testTransactionRollback() {
        try (Transaction tx = grid(0).transactions().txStart()) {
            putThreeValuesAndCreateSavepoints(tx);
            tx.releaseCheckpoint("s3");
            tx.rollbackToSavepoint("s2");
            tx.rollback();
        }
        assertEquals(null, cache.get(2));
    }

    /**
     *
     * @param tx
     */
    private void putThreeValuesAndCreateSavepoints(Transaction tx) {
        cache.put(2, 1);
        tx.savepoint("s1");
        cache.put(2, 2);
        tx.savepoint("s2");
        cache.put(2, 3);
        tx.savepoint("s3");
    }

}