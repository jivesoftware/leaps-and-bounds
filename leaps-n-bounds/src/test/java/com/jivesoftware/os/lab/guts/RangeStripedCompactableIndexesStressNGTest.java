package com.jivesoftware.os.lab.guts;

import com.google.common.io.Files;
import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.LABRawhide;
import com.jivesoftware.os.lab.api.NoOpFormatTransformerProvider;
import com.jivesoftware.os.lab.api.RawEntryFormat;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
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
        boolean useMemMap = true;
        int entriesBetweenLeaps = 4096;
        long splitWhenKeysTotalExceedsNBytes = 8 * 1024 * 1024; //1024 * 1024 * 10;
        long splitWhenValuesTotalExceedsNBytes = -1;
        long splitWhenValuesAndKeysTotalExceedsNBytes = -1;

        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        RangeStripedCompactableIndexes indexs = new RangeStripedCompactableIndexes(destroy,
            root,
            "test",
            useMemMap,
            entriesBetweenLeaps,
            splitWhenKeysTotalExceedsNBytes,
            splitWhenValuesTotalExceedsNBytes,
            splitWhenValuesAndKeysTotalExceedsNBytes,
            NoOpFormatTransformerProvider.NO_OP,
            new SimpleRawhide(),
            new AtomicReference<>(new RawEntryFormat(0, 0)),
            leapsCache);

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

            int[] hits = {0};
            int[] misses = {0};
            RawEntryStream hitsAndMisses = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry, offset, length) -> {
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
                byte[] longAsBytes = UIO.longBytes(longKey);
                indexs.rangeTx(-1, longAsBytes, longAsBytes, -1, -1, (index, fromKey, toKey, acquire, hydrateValues) -> {
                    GetRaw getRaw = IndexUtil.get(acquire);

                    try {

                        UIO.longBytes(longKey, key, 0);
                        getRaw.get(key, hitsAndMisses);

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

        for (int b = 0; b < numBatches; b++) {

            RawMemoryIndex index = new RawMemoryIndex(destroy, new AtomicLong(), new LABRawhide());
            long lastKey = IndexTestUtils.append(rand, index, 0, maxKeyIncrement, batchSize, null);
            indexs.append(index, fsync);

            int debt = indexs.debt();
            if (debt < minMergeDebt) {
                continue;
            }

            while (true) {
                List<Callable<Void>> compactors = indexs.buildCompactors(fsync, minMergeDebt);
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
