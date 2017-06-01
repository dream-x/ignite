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

package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import java.util.ArrayList;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 *
 */
public abstract class TxSavepointsTest extends GridCommonAbstractTest {

    private final static ArrayList<TestEntry> testEntries = new ArrayList<>();
    static {
        String one[] = {"put1", "remove"};
        String two[] = {"put2", "nothing"};
        String three[] = {"put3", "remove", "put4AndRemove", "nothing"};
        int idx = 0;
        for (int i = 0; i < one.length; i++) {
            for (int j = 0; j < two.length; j++) {
                for (int k = 0; k < three.length; k++) {
                    testEntries.add(new TestEntry(++idx, one[i], two[j], three[k]));
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disc = new TcpDiscoverySpi();

        disc.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(disc);

        return cfg;
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param cacheMode Cache mode.
     * @return Cache configuration.
     */
    protected CacheConfiguration<Integer, Integer> cacheConfig(CacheAtomicityMode atomicityMode, CacheMode cacheMode) {
        CacheConfiguration<Integer, Integer> cfg  = new CacheConfiguration<>();

        cfg.setAtomicityMode(atomicityMode);

        cfg.setCacheMode(cacheMode);

        cfg.setBackups(1);

        cfg.setName("txSvp_" + atomicityMode + "_" + cacheMode);

        return cfg;
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param cacheMode Cache mode.
     * @throws Exception If test fails.
     */
    protected void checkSavepoints(CacheAtomicityMode atomicityMode, CacheMode cacheMode) throws Exception {
        startGrid(0);

        CacheConfiguration<Integer, Integer> cfg = cacheConfig(atomicityMode, cacheMode);

        IgniteCache<Integer, Integer> cache = grid(0).createCache(cfg);

        for (TransactionConcurrency concurrency : TransactionConcurrency.values())
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                if (isDebug())
                    info(concurrency + " " + isolation);

                doActionsBeforeTx(cache);

                try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
                    String errMsg = "Broken rollback to savepoint in " + concurrency + " " + isolation + " transaction.";

                    doActivity("s1", tx, cache);

                    checkResult(errMsg, cfg, cache);

                    doActivity("s2", tx, cache);

                    checkResult(errMsg, cfg, cache);

                    tx.commit();
                }

                String errMsg = "Broken commit after savepoint in " + concurrency + " " + isolation + " transaction.";

                checkResult(errMsg, cfg, cache);
            }
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param cacheMode Cache mode.
     * @throws Exception If test fails.
     */
    protected void checkSavepointsWithTwoCaches(CacheAtomicityMode atomicityMode, CacheMode cacheMode) throws Exception {
        startGrid(0);
        startGrid(1);
        startGrid(2);

        CacheConfiguration<Integer, Integer> cfg = cacheConfig(atomicityMode, cacheMode);

        IgniteCache<Integer, Integer> cache1 = grid(0).createCache(cfg.setName("First Cache"));
        IgniteCache<Integer, Integer> cache2 = grid(0).createCache(new CacheConfiguration<Integer, Integer>(cfg).setName("Second Cache"));

        for (TransactionConcurrency concurrency : TransactionConcurrency.values())
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                if (isDebug())
                    info(concurrency + " " + isolation);

                doActionsBeforeTx(cache1);
                doActionsBeforeTx(cache2);

                try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
                    String errMsg = "Broken rollback to savepoint in " + concurrency + " " + isolation + " transaction in cache";

                    doActivity("s1", tx, cache1, cache2);

                    checkResult(errMsg + "1.", cfg, cache1);
                    checkResult(errMsg + "2.", cfg, cache2);

                    doActivity("s2", tx, cache1, cache2);

                    checkResult(errMsg + "1.", cfg, cache1);
                    checkResult(errMsg + "2.", cfg, cache2);

                    tx.commit();
                }

                String errMsg = "Broken commit after savepoint in " + concurrency + " " + isolation + " transaction in cache";

                checkResult(errMsg + "1.", cfg, cache1);
                checkResult(errMsg + "2.", cfg, cache2);
            }
    }

    /**
     * @param savepointName Savepoint which will be created and rolled back to.
     * @param tx Transaction.
     * @param caches Caches for some operations.
     */
    private void doActivity(String savepointName, Transaction tx, IgniteCache<Integer, Integer>... caches) {
        for (IgniteCache<Integer, Integer> cache : caches) {
            doActionsBeforeSavepoint(cache);

            tx.savepoint(savepointName);

            doActionsAfterSavepoint(cache);

            tx.rollbackToSavepoint(savepointName);
        }
    }

    /**
     * @param cache Cache to work with.
     */
    private void doActionsBeforeTx(IgniteCache<Integer, Integer> cache) {
        for (TestEntry e : testEntries) {
            doAction(cache, e.index, e.actionBeforeTx);
        }
    }

    /**
     * @param cache Cache to work with.
     */
    private void doActionsBeforeSavepoint(IgniteCache<Integer, Integer> cache) {
        for (TestEntry e : testEntries) {
            doAction(cache, e.index, e.actionBeforeSavepoint);
        }
    }

    /**
     * @param cache Cache to work with.
     */
    private void doActionsAfterSavepoint(IgniteCache<Integer, Integer> cache) {
        for (TestEntry e : testEntries) {
            doAction(cache, e.index, e.actionAfterSavepoint);
        }
    }

    /**
     * @param errMsg Error message.
     * @param cfg Cache configuration.
     * @param cache Cache to work with.
     */
    private void checkResult(String errMsg, CacheConfiguration cfg, IgniteCache<Integer, Integer> cache) {
        for (TestEntry e : testEntries) {
            if (cfg.getAtomicityMode() == TRANSACTIONAL)
                assertEquals(errMsg, e.resultTransactional, cache.get(e.index));
            else
                assertEquals(errMsg, e.resultAtomic, cache.get(e.index));
        }
    }

    /** */
    private static class TestEntry {
        private final Integer index;
        private final String actionBeforeTx;
        private final String actionBeforeSavepoint;
        private final String actionAfterSavepoint;
        private final Integer resultTransactional;
        private final Integer resultAtomic;

        private TestEntry(int index, String actionBeforeTx, String actionBeforeSavepoint, String actionAfterSavepoint) {
            this.index = index;
            this.actionBeforeTx = actionBeforeTx;
            this.actionBeforeSavepoint = actionBeforeSavepoint;
            this.actionAfterSavepoint = actionAfterSavepoint;

            if (actionBeforeSavepoint.equals("put2")) {
                resultTransactional = 2;
            } else {
                if (actionBeforeTx.equals("put1"))
                    resultTransactional = 1;
                else
                    resultTransactional = null;
            }

            if (actionAfterSavepoint.equals("put3")) {
                resultAtomic = 3;
            } else {
                if (actionAfterSavepoint.equals("remove") || actionAfterSavepoint.equals("put4AndRemove")) {
                    resultAtomic = null;
                } else {
                    if (actionBeforeSavepoint.equals("put2")) {
                        resultAtomic = 2;
                    } else {
                        if (actionBeforeTx.equals("put1"))
                            resultAtomic = 1;
                        else
                            resultAtomic = null;
                    }
                }
            }
        }
    }

    /**
     * @param cache Cache to work with.
     * @param key Key to be changed in cache.
     * @param action What to do with given key.
     */
    private void doAction(IgniteCache<Integer, Integer> cache, Integer key, String action) {
        switch (action) {
            case "put1":
                cache.put(key, 1);

                break;

            case "put2":
                cache.put(key, 2);

                break;

            case "put3":
                cache.put(key, 3);

                break;

            case "remove":
                cache.remove(key);

                break;

            case "put4AndRemove":
                cache.put(key, 4);
                cache.remove(key);

                break;

            default:
                break;
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }
}