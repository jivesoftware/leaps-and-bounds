package com.jivesoftware.os.lab.guts;

import com.google.common.io.Files;
import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.LABStats;
import com.jivesoftware.os.lab.TestUtils;
import com.jivesoftware.os.lab.api.NoOpFormatTransformerProvider;
import com.jivesoftware.os.lab.api.RawEntryFormat;
import com.jivesoftware.os.lab.api.rawhide.LABRawhide;
import com.jivesoftware.os.lab.api.rawhide.Rawhide;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.File;
import java.text.NumberFormat;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableLong;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class IndexStressNGTest {

    private final NumberFormat format = NumberFormat.getInstance();
    private final Rawhide rawhide = LABRawhide.SINGLETON;

    @Test(enabled = false)
    public void stress() throws Exception {
        ExecutorService destroy = Executors.newSingleThreadExecutor();

        Random rand = new Random(12345);

        long start = System.currentTimeMillis();
        CompactableIndexes indexs = new CompactableIndexes(new LABStats(), rawhide);
        int count = 0;

        boolean fsync = true;
        boolean concurrentReads = true;
        int numBatches = 100;
        int batchSize = 1000;
        int maxKeyIncrement = 2000;
        int entriesBetweenLeaps = 1024;
        int minMergeDebt = 4;

        AtomicLong merge = new AtomicLong();
        MutableLong maxKey = new MutableLong();
        MutableBoolean running = new MutableBoolean(true);
        MutableBoolean merging = new MutableBoolean(true);
        MutableLong stopGets = new MutableLong(System.currentTimeMillis() + 60_000);

        File root = Files.createTempDir();
        AtomicLong waitForDebtToDrain = new AtomicLong();
        Future<Object> mergering = Executors.newSingleThreadExecutor().submit(() -> {
            while (running.isTrue()) {

                try {

                    Callable<Void> compactor = indexs.compactor(new LABStats(), "test", -1, -1, -1, null, minMergeDebt, fsync,
                        (rawhideName, minimumRun1, fsync1, callback) -> callback.call(minimumRun1, fsync1,
                            (id, worstCaseCount) -> {

                                long m = merge.incrementAndGet();
                                int maxLeaps = RangeStripedCompactableIndexes.calculateIdealMaxLeaps(worstCaseCount, entriesBetweenLeaps);
                                File mergingFile = id.toFile(root);
                                return new LABAppendableIndex(new LongAdder(),
                                    id,
                                    new AppendOnlyFile(mergingFile),
                                    maxLeaps,
                                    entriesBetweenLeaps,
                                    rawhide,
                                    new RawEntryFormat(0, 0),
                                    NoOpFormatTransformerProvider.NO_OP,
                                    TestUtils.indexType,
                                    0.75d);
                            },
                            (ids) -> {
                                File mergedFile = ids.get(0).toFile(root);
                                LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
                                return new ReadOnlyIndex(destroy, ids.get(0), new ReadOnlyFile(mergedFile), NoOpFormatTransformerProvider.NO_OP,
                                    rawhide, leapsCache);
                            }));
                    if (compactor != null) {
                        waitForDebtToDrain.incrementAndGet();
                        compactor.call();
                        synchronized (waitForDebtToDrain) {
                            waitForDebtToDrain.decrementAndGet();
                            waitForDebtToDrain.notifyAll();
                        }
                    } else {
                        Thread.sleep(100);
                    }
                } catch (Exception x) {
                    x.printStackTrace();
                    Thread.sleep(10_000);
                }
            }
            return null;

        });

        Future<Object> pointGets = Executors.newSingleThreadExecutor().submit(() -> {

            int[] hits = {0};
            int[] misses = {0};
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
            while (running.isTrue() || stopGets.longValue() > System.currentTimeMillis()) {
                if (maxKey.intValue() < batchSize) {
                    Thread.sleep(10);
                    continue;
                }
                while (indexs.tx(-1, null, null, (index, fromKey, toKey, acquired, hydrateValues) -> {
                    PointInterleave pointInterleave = new PointInterleave(acquired, key, rawhide, true);

                    try {

                        int longKey = rand.nextInt(maxKey.intValue());
                        UIO.longBytes(longKey, key, 0);
                        pointInterleave.next(hitsAndMisses);

                        if ((hits[0] + misses[0]) % logInterval == 0) {
                            return false;
                        }

                        //Thread.sleep(1);
                    } catch (Exception x) {
                        x.printStackTrace();
                        Thread.sleep(10);
                    }
                    return true;
                }, true)) {
                }

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

        int maxLeaps = RangeStripedCompactableIndexes.calculateIdealMaxLeaps(batchSize, entriesBetweenLeaps);
        for (int b = 0; b < numBatches; b++) {

            IndexRangeId id = new IndexRangeId(b, b, 0);
            File indexFiler = File.createTempFile("s-index-merged-" + b, ".tmp");

            long startMerge = System.currentTimeMillis();
            LABAppendableIndex write = new LABAppendableIndex(new LongAdder(),
                id,
                new AppendOnlyFile(indexFiler),
                maxLeaps,
                entriesBetweenLeaps,
                rawhide,
                new RawEntryFormat(0, 0),
                NoOpFormatTransformerProvider.NO_OP,
                TestUtils.indexType,
                0.75d);
            BolBuffer keyBuffer = new BolBuffer();
            long lastKey = TestUtils.append(rand, write, 0, maxKeyIncrement, batchSize, null, keyBuffer);
            write.closeAppendable(fsync);

            maxKey.setValue(Math.max(maxKey.longValue(), lastKey));
            LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
            indexs.append(
                new ReadOnlyIndex(destroy, id, new ReadOnlyFile(indexFiler), NoOpFormatTransformerProvider.NO_OP, rawhide, leapsCache));

            count += batchSize;

            System.out.println("Insertions:" + format.format(count) + " ips:" + format.format(
                ((count / (double) (System.currentTimeMillis() - start))) * 1000) + " elapse:" + format.format(
                    (System.currentTimeMillis() - startMerge)) + " mergeDebut:" + indexs.debt());

            if (indexs.debt() > 10) {
                synchronized (waitForDebtToDrain) {
                    if (waitForDebtToDrain.get() > 0) {
                        System.out.println("Waiting because debt is two high....");
                        waitForDebtToDrain.wait();
                    }
                }
            }
        }

        running.setValue(false);
        mergering.get();
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
