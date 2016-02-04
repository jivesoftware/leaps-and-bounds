package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.File;
import java.text.NumberFormat;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.lang.mutable.MutableLong;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class IndexStressNGTest {

    NumberFormat format = NumberFormat.getInstance();

    @Test(enabled = true)
    public void stress() throws Exception {
        ExecutorService destroy = Executors.newSingleThreadExecutor();

        Random rand = new Random();

        long start = System.currentTimeMillis();
        MergeableIndexes indexs = new MergeableIndexes();
        int count = 0;

        boolean concurrentReads = false;
        int numBatches = 1000;
        int batchSize = 10_000;
        int maxKeyIncrement = 2000;
        int entriesBetweenLeaps = 1024;

        AtomicLong merge = new AtomicLong();
        MutableLong maxKey = new MutableLong();
        MutableBoolean running = new MutableBoolean(true);
        MutableBoolean merging = new MutableBoolean(true);
        MutableLong stopGets = new MutableLong(System.currentTimeMillis() + 4_000);

        Object waitForDebtToDrom = new Object();
        Future<Object> mergering = Executors.newSingleThreadExecutor().submit(() -> {
            while (running.isTrue()) {

                try {
                    MergeableIndexes.Merger merger = indexs.buildMerger((id, worstCaseCount) -> {

                        long m = merge.incrementAndGet();
                        int maxLeaps = IndexUtil.calculateIdealMaxLeaps(worstCaseCount, entriesBetweenLeaps);
                        File mergeIndexFiler = File.createTempFile("d-index-merged-" + m, ".tmp");
                        return new WriteLeapsAndBoundsIndex(id, new IndexFile(mergeIndexFiler, "rw", true),
                            maxLeaps, entriesBetweenLeaps);
                    }, (id, index) -> {
                        return new LeapsAndBoundsIndex(destroy, id, new IndexFile(index.getIndex().getFile(), "r", true));
                    });

                    if (merger != null) {
                        merger.call();
                        synchronized (waitForDebtToDrom) {
                            waitForDebtToDrom.notifyAll();
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
            RawEntryStream hitsAndMisses = (rawEntry, offset, length) -> {
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

            MergeableIndexes.Reader reader = indexs.reader();

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
                reader.tx(acquire -> {
                    GetRaw pointer = IndexUtil.get(acquire);

                    try {

                        int longKey = rand.nextInt(i);
                        UIO.longBytes(longKey, key, 0);
                        pointer.get(key, hitsAndMisses);

                        if ((hits[0] + misses[0]) % logInterval == 0) {
                            return null;
                        }

                        //Thread.sleep(1);
                    } catch (Exception x) {
                        x.printStackTrace();
                        Thread.sleep(10);
                    }
                    return null;
                });

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

        int maxLeaps = IndexUtil.calculateIdealMaxLeaps(batchSize, entriesBetweenLeaps);
        for (int b = 0; b < numBatches; b++) {

            IndexRangeId id = new IndexRangeId(b, b, 0);
            File indexFiler = File.createTempFile("s-index-merged-" + b, ".tmp");

            long startMerge = System.currentTimeMillis();
            WriteLeapsAndBoundsIndex write = new WriteLeapsAndBoundsIndex(id,
                new IndexFile(indexFiler, "rw", true), maxLeaps, entriesBetweenLeaps);
            long lastKey = IndexTestUtils.append(rand, write, 0, maxKeyIncrement, batchSize, null);
            write.close();

            maxKey.setValue(Math.max(maxKey.longValue(), lastKey));
            indexs.append(new LeapsAndBoundsIndex(destroy, id, new IndexFile(indexFiler, "r", true)));

            count += batchSize;

            System.out.println("Insertions:" + format.format(count) + " ips:" + format.format(
                ((count / (double) (System.currentTimeMillis() - start))) * 1000) + " elapse:" + format.format(
                    (System.currentTimeMillis() - startMerge)) + " mergeDebut:" + indexs.hasMergeDebt());

            if (indexs.hasMergeDebt() > 10) {
                synchronized (waitForDebtToDrom) {
                    System.out.println("Waiting because debt is two high....");
                    waitForDebtToDrom.wait();
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
