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
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.GridTestSafeThreadFactory;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/** */
public abstract class TxSavepointsTransactionalCacheTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** */
    private CacheConfiguration<Integer, Integer> getConfig() {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>();

        cfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheMode(cacheMode());

        cfg.setName(cacheMode().name());

        return cfg;
    }

    /** Override this method to use different cache modes for tests.*/
    protected abstract CacheMode cacheMode();

    /**
     * @throws Exception If failed.
     */
    public void testGet() throws Exception {
        IgniteCache<Integer, Integer> cache = grid().getOrCreateCache(getConfig());

        GridTestSafeThreadFactory factory = new GridTestSafeThreadFactory(cacheMode().name()+"_get");

        for (TransactionConcurrency concurrency : TransactionConcurrency.values())
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                cache.remove(1);

                try (Transaction tx = grid().transactions().txStart(concurrency, isolation)) {
                    tx.savepoint("sp");

                    assertEquals("Broken savepoint in " + concurrency + " " + isolation +
                        " transaction.", null, cache.get(1));

                    tx.rollbackToSavepoint("sp");

                    Thread t = factory.newThread(new Runnable() {
                        @Override public void run() {
                            cache.put(1, 1);
                        }
                    });

                    t.start();

                    t.join();

                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                        " " + isolation + " transaction.", (Integer) 1, cache.get(1));
                }
            }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPut() throws Exception {
        IgniteCache<Integer, Integer> cache = grid().getOrCreateCache(getConfig());

        GridTestSafeThreadFactory factory = new GridTestSafeThreadFactory(cacheMode().name()+"_put");

        for (TransactionConcurrency concurrency : TransactionConcurrency.values())
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                cache.remove(1);

                try (Transaction tx = grid().transactions().txStart(concurrency, isolation)) {
                    tx.savepoint("sp");

                    assertTrue(cache.putIfAbsent(1, 0));

                    Thread t = factory.newThread(new Runnable() {
                        @Override public void run() {
                            assertTrue(cache.putIfAbsent(1, 1));
                        }
                    });

                    t.start();

                    //we need to wait some time to make sure that second thread trying to get lock.
                    for (int i = 2; i < 1_000; i++)
                        cache.putIfAbsent(i, 0);

                    tx.rollbackToSavepoint("sp");

                    t.join();

                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                        " " + isolation + " transaction.",  (Integer) 1, cache.get(1));

                    tx.commit();
                }

                assertEquals("Broken rollback to savepoint in " + concurrency + " " + isolation +
                    " transaction.",  (Integer) 1, cache.get(1));
            }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemove() throws Exception {
        IgniteCache<Integer, Integer> cache = grid().getOrCreateCache(getConfig());

        GridTestSafeThreadFactory factory = new GridTestSafeThreadFactory(cacheMode().name()+"_remove");

        for (TransactionConcurrency concurrency : TransactionConcurrency.values())
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                cache.put(1, 1);

                try (Transaction tx = grid().transactions().txStart(concurrency, isolation)) {
                    tx.savepoint("sp");

                    assertTrue(cache.remove(1));

                    Thread t = factory.newThread(new Runnable() {
                        @Override public void run() {
                            assertTrue(cache.remove(1, 1));
                        }
                    });

                    t.start();

                    //we need to wait some time to make sure that second thread trying to get lock.
                    for (int i = 2; i < 1_000; i++)
                        cache.putIfAbsent(i, 0);

                    tx.rollbackToSavepoint("sp");

                    t.join();

                    assertEquals("Broken multithreaded rollback to savepoint in " + concurrency +
                        " " + isolation + " transaction.",  null, cache.get(1));

                    tx.commit();
                }

                assertEquals("Broken rollback to savepoint in " + concurrency + " " + isolation +
                    " transaction.",  null, cache.get(1));
            }
    }
}