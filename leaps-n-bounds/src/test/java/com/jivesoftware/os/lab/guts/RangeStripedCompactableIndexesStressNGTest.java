package com.jivesoftware.os.lab.guts;

import com.google.common.io.Files;
import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.LABStats;
import com.jivesoftware.os.lab.LabHeapPressure;
import com.jivesoftware.os.lab.TestUtils;
import com.jivesoftware.os.lab.api.NoOpFormatTransformerProvider;
import com.jivesoftware.os.lab.api.RawEntryFormat;
import com.jivesoftware.os.lab.api.rawhide.LABRawhide;
import com.jivesoftware.os.lab.guts.allocators.LABAppendOnlyAllocator;
import com.jivesoftware.os.lab.guts.allocators.LABConcurrentSkipListMap;
import com.jivesoftware.os.lab.guts.allocators.LABConcurrentSkipListMemory;
import com.jivesoftware.os.lab.guts.allocators.LABIndexableMemory;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.text.NumberFormat;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableLong;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class RangeStripedCompactableIndexesStressNGTest {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    NumberFormat format = NumberFormat.getInstance();

    @Test(enabled = false)
    public void stress() throws Exception {
        ExecutorService destroy = Executors.newSingleThreadExecutor();

        Random rand = new Random(12345);

        long start = System.currentTimeMillis();

        File root = Files.createTempDir();
        int entriesBetweenLeaps = 4096;
        long splitWhenKeysTotalExceedsNBytes = 8 * 1024 * 1024; //1024 * 1024 * 10;
        long splitWhenValuesTotalExceedsNBytes = -1;
        long splitWhenValuesAndKeysTotalExceedsNBytes = -1;

        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        LABStats labStats = new LABStats();
        RangeStripedCompactableIndexes indexs = new RangeStripedCompactableIndexes(labStats,
            destroy,
            root,
            "test",
            entriesBetweenLeaps,
            splitWhenKeysTotalExceedsNBytes,
            splitWhenValuesTotalExceedsNBytes,
            splitWhenValuesAndKeysTotalExceedsNBytes,
            NoOpFormatTransformerProvider.NO_OP,
            LABRawhide.SINGLETON,
            new AtomicReference<>(new RawEntryFormat(0, 0)),
            leapsCache,
            false,
            TestUtils.indexType,
            0.75d);

        int count = 0;

        boolean fsync = true;
        boolean concurrentReads = false;
        int numBatches = 1000;
        int batchSize = 10_000;
        int maxKeyIncrement = 2000;
        int minMergeDebt = 4;

        AtomicLong merge = new AtomicLong();
        MutableLong maxKey = new MutableLong();
        MutableBoolean running = new MutableBoolean(true);
        MutableBoolean merging = new MutableBoolean(true);
        MutableLong stopGets = new MutableLong(System.currentTimeMillis() + 4_000);

        Future<Object> pointGets = Executors.newSingleThreadExecutor().submit(() -> {

            int[] hits = { 0 };
            int[] misses = { 0 };
            RawEntryStream hitsAndMisses = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                if (rawEntry != null) {
                    hits[0]++;
                } else {
                    misses[0]++;
                }
                return true;
            };
            long best = Long.MAX_VALUE;
            long total = 0;
            long samples = 0;
            byte[] key = new byte[8];

            if (!concurrentReads) {
                while (merging.isTrue() || running.isTrue()) {
                    Thread.sleep(100);
                }
            }

            int logInterval = 100_000;
            long getStart = System.currentTimeMillis();
            while (stopGets.longValue() > System.currentTimeMillis()) {
                int i = maxKey.intValue();
                if (i == 0) {
                    Thread.sleep(10);
                    continue;
                }
                int longKey = rand.nextInt(i);
                byte[] longAsBytes = UIO.longBytes(longKey, new byte[8], 0);
                indexs.rangeTx(-1, longAsBytes, longAsBytes, -1, -1, (index, fromKey, toKey, acquired, hydrateValues) -> {
                    PointInterleave pointInterleave = new PointInterleave(acquired, key, LABRawhide.SINGLETON, true);

                    try {

                        UIO.longBytes(longKey, key, 0);
                        pointInterleave.next( hitsAndMisses);

                        if ((hits[0] + misses[0]) % logInterval == 0) {
                            return true;
                        }

                        //Thread.sleep(1);
                    } catch (Exception x) {
                        x.printStackTrace();
                        Thread.sleep(10);
                    }
                    return true;
                }, true);

                long getEnd = System.currentTimeMillis();
                long elapse = (getEnd - getStart);
                total += elapse;
                samples++;
                if (elapse < best) {
                    best = elapse;
                }

                System.out.println(
                    "Hits:" + hits[0] + " Misses:" + misses[0] + " Elapse:" + elapse + " Best:" + rps(logInterval, best) + " Avg:" + rps(logInterval,
                        (long) (total / (double) samples)));
                hits[0] = 0;
                misses[0] = 0;
                getStart = getEnd;
            }
            return null;

        });

        AtomicLong ongoingCompactions = new AtomicLong();
        Object compactLock = new Object();
        ExecutorService compact = Executors.newCachedThreadPool();
        BolBuffer keyBuffer = new BolBuffer();
        BolBuffer entryBuffer = new BolBuffer();
        BolBuffer entryKeyBuffer = new BolBuffer();

        for (int b = 0; b < numBatches; b++) {
            LabHeapPressure labHeapPressure = new LabHeapPressure(labStats,
                LABEnvironment.buildLABHeapSchedulerThreadPool(1),
                "default",
                -1,
                -1,
                new AtomicLong(),
                LabHeapPressure.FreeHeapStrategy.mostBytesFirst);

            LABMemoryIndex index = new LABMemoryIndex(destroy,
                labHeapPressure,
                labStats,
                LABRawhide.SINGLETON,
                new LABConcurrentSkipListMap(labStats,
                    new LABConcurrentSkipListMemory(LABRawhide.SINGLETON,
                        new LABIndexableMemory(new LABAppendOnlyAllocator("test", 2))
                    ),
                    new StripingBolBufferLocks(1024)
                ));
            long lastKey = TestUtils.append(rand, index, 0, maxKeyIncrement, batchSize, null, keyBuffer);
            indexs.append("test", index, fsync, keyBuffer, entryBuffer, entryKeyBuffer);

            int debt = indexs.debt();
            if (debt < minMergeDebt) {
                continue;
            }

            while (true) {
                List<Callable<Void>> compactors = indexs.buildCompactors("test", fsync, minMergeDebt);
                if (compactors != null && !compactors.isEmpty()) {
                    for (Callable<Void> compactor : compactors) {
                        //LOG.info("Scheduling async compaction:{} for index:{} debt:{}", compactors, indexs, debt);
                        ongoingCompactions.incrementAndGet();
                        compact.submit(() -> {
                            try {
                                compactor.call();
                            } catch (Exception x) {
                                LOG.error("Failed to compact " + indexs, x);
                            } finally {
                                synchronized (compactLock) {
                                    ongoingCompactions.decrementAndGet();
                                    compactLock.notifyAll();
                                }
                            }
                            return null;
                        });
                    }
                }

                if (debt >= minMergeDebt) {
                    synchronized (compactLock) {
                        if (ongoingCompactions.get() > 0) {
                            LOG.info("Waiting because debt it do high index:{} debt:{}", compactors, debt);
                            compactLock.wait();
                        } else {
                            break;
                        }
                    }
                    debt = indexs.debt();
                } else {
                    break;
                }
            }

            maxKey.setValue(Math.max(maxKey.longValue(), lastKey));

            count += batchSize;

            System.out.println("Insertions:" + format.format(count) + " ips:" + format.format(
                ((count / (double) (System.currentTimeMillis() - start))) * 1000) + " mergeDebut:" + indexs.debt());
        }

        running.setValue(false);
        /*System.out.println("Sleeping 10 sec before gets...");
        Thread.sleep(10_000L);*/
        merging.setValue(false);
        System.out.println(
            " **************   Total time to add " + (numBatches * batchSize) + " including all merging: "
                + (System.currentTimeMillis() - start) + " millis *****************");

        pointGets.get();

        System.out.println("Done. " + (System.currentTimeMillis() - start));

    }

    private long rps(long logInterval, long elapse) {
        return (long) ((logInterval / (double) elapse) * 1000);
    }

}
