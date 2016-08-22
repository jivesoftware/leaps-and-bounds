package com.jivesoftware.os.lab;

import com.google.common.io.Files;
import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.api.FixedWidthRawhide;
import com.jivesoftware.os.lab.api.Keys;
import com.jivesoftware.os.lab.api.MemoryRawEntryFormat;
import com.jivesoftware.os.lab.api.NoOpFormatTransformerProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueIndexConfig;
import com.jivesoftware.os.lab.guts.Leaps;
import com.jivesoftware.os.lab.guts.RangeStripedCompactableIndexes;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LABStress {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    @Test(enabled = true)
    public void stressWrites() throws Exception {

        File root = Files.createTempDir();
        ValueIndex index = createIndex(root);

        int totalCardinality = 100_000_000;

        System.out.println("Sample, Writes, Writes/Sec, WriteElapse, Reads, Reads/Sec, ReadElapse, Hits, Miss, Merged, Split, ReadAmplification");

        String write = stress("warm:jit",
            index,
            totalCardinality,
            800_000, // writesPerSecond
            1_000_000, //writeCount
            true, // writeMonotonicly
            1, //readForNSeconds
            1_000_000, // readCount
            false); // removes

        List<Future<Object>> futures = index.commit(true, false);
        for (Future<Object> future : futures) {
            future.get();
        }

        System.out.println("\n\n");

        System.out.println(write);
        //System.out.println(read);
        System.out.println("size:" + calculateFileSize(root) + "bytes " + root);
        System.out.println("\n\n");

        System.gc();
        System.runFinalization();

        System.out.println("-------------------------------");

        root = Files.createTempDir();
        index = createIndex(root);

        // ---
        System.out.println("Sample, Writes, Writes/Sec, WriteElapse, Reads, Reads/Sec, ReadElapse, Hits, Miss, Merged, Split, ReadAmplification");

        totalCardinality = 100_000_000;

        write = stress("stress:RW",
            index,
            totalCardinality,
            800_000, // writesPerSecond
            50_000_000, //writeCount
            true, // writeMonotonicly
            0, //readForNSeconds
            0, // readCount
            false); // removes

        System.out.println("\n\n");
        ((LAB) index).auditRanges((key) -> "" + UIO.bytesLong(key));
        System.out.println("\n\n");

        System.out.println(write);
        System.out.println("size:" + calculateFileSize(root) + "bytes " + root);
        System.out.println("\n\n");

        System.out.println("COMMIT ALL");
        futures = index.commit(true, true);
        for (Future<Object> future : (futures != null) ? futures : Collections.<Future<Object>>emptyList()) {
            future.get();
        }
        System.out.println("COMMITED ALL");

        System.out.println("COMPACT ALL");
        futures = index.compact(true, 0, 0, true);
        for (Future<Object> future : (futures != null) ? futures : Collections.<Future<Object>>emptyList()) {
            future.get();
        }
        System.out.println("COMPACTED ALL");

        System.out.println("Sample, Writes, Writes/Sec, WriteElapse, Reads, Reads/Sec, ReadElapse, Hits, Miss, Merged, Split, ReadAmplification");

        write = stress("stress:R",
            index,
            totalCardinality,
            0, // writesPerSecond
            0, //writeCount
            false, // writeMonotonicly
            10, //readForNSeconds
            50_000_000, // readCount
            false); // removes

        System.out.println("\n\n");
        ((LAB) index).auditRanges((key) -> "" + UIO.bytesLong(key));
        System.out.println("\n\n");

        System.out.println(write);
        System.out.println("size:" + calculateFileSize(root) + "bytes " + root);
        System.out.println("\n\n");

    }

    private ValueIndex createIndex(File root) throws Exception {
        System.out.println("Created root " + root);
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100_000, 8);
        LabHeapPressure labHeapPressure = new LabHeapPressure(LABEnvironment.buildLABHeapSchedulerThreadPool(1), "default", 1024 * 1024 * 10, 1024 * 1024 * 10,
            new AtomicLong());
        LABEnvironment env = new LABEnvironment(LABEnvironment.buildLABSchedulerThreadPool(1),
            LABEnvironment.buildLABCompactorThreadPool(4), // compact
            LABEnvironment.buildLABDestroyThreadPool(1), // destroy
            "wal",
            1024 * 1024 * 10,
            1000,
            1024 * 1024 * 10,
            1024 * 1024 * 10,
            root, // rootFile
            labHeapPressure,
            4, // minMergeDebt
            8, // maxMergeDebt
            leapsCache,
            true);

        env.register("8x8fixedWidthRawhide", new FixedWidthRawhide(8, 8));

        System.out.println("Created env");
        ValueIndex index = env.open(new ValueIndexConfig("foo",
            1024 * 10, // entriesBetweenLeaps
            1024 * 1024 * 512, // maxHeapPressureInBytes
            -1, // splitWhenKeysTotalExceedsNBytes
            -1, // splitWhenValuesTotalExceedsNBytes
            1024 * 1024 * 10, // splitWhenValuesAndKeysTotalExceedsNBytes
            NoOpFormatTransformerProvider.NAME,
            "8x8fixedWidthRawhide", //new LABRawhide(),
            MemoryRawEntryFormat.NAME));
        return index;
    }

    private String stress(String name,
        ValueIndex index,
        int totalCardinality,
        int writesPerSecond,
        int writeCount,
        boolean writeMonotonicly,
        int readForNSeconds,
        int readCount,
        boolean removes) throws Exception {

        AtomicLong version = new AtomicLong();
        AtomicLong value = new AtomicLong();
        AtomicLong count = new AtomicLong();

        long totalWriteTime = 0;
        long totalWrites = 0;

        long totalReadTime = 0;
        long totalReads = 0;

        long totalHits = 0;
        long totalMiss = 0;

        Random rand = new Random(12345);
        AtomicLong monotonic = new AtomicLong();

        int c = 0;
        while ((writeCount > 0 && totalWrites < writeCount) || (readCount > 0 && totalReads < readCount)) {
            long start = System.currentTimeMillis();
            long writeElapse = 0;
            double writeRate = 0;
            if (writeCount > 0 && totalWrites < writeCount) {
                long preWriteCount = count.get();
                index.append((stream) -> {
                    for (int i = 0; i < writesPerSecond; i++) {
                        count.incrementAndGet();
                        long key = rand.nextInt(totalCardinality);
                        if (writeMonotonicly) {
                            key = monotonic.incrementAndGet();
                        }
                        stream.stream(-1,
                            UIO.longBytes(key),
                            System.currentTimeMillis(),
                            (removes) ? rand.nextBoolean() : false,
                            version.incrementAndGet(),
                            UIO.longBytes(value.incrementAndGet()));
                    }
                    return true;
                }, true);

                //index.commit(true);
                long wrote = count.get() - preWriteCount;
                totalWrites += wrote;
                //System.out.println("Commit Elapse:" + (System.currentTimeMillis() - start));
                writeElapse = (System.currentTimeMillis() - start);
                totalWriteTime += writeElapse;
                writeRate = (double) wrote * 1000 / (writeElapse);
                if (writeElapse < 1000) {
                    Thread.sleep(1000 - writeElapse);
                }
            } else {
                monotonic.addAndGet(readCount);
            }

            start = System.currentTimeMillis();
            long readElapse = 0;
            double readRate = 0;
            AtomicLong misses = new AtomicLong();
            AtomicLong hits = new AtomicLong();
            if (readCount > 0 && totalReads < readCount) {
                LAB.pointTxCalled.set(0);
                LAB.pointTxIndexCount.set(0);
                long s = start;

                while (System.currentTimeMillis() - s < (1000 * readForNSeconds)) {

                    index.get((Keys.KeyStream keyStream) -> {
                        long k = rand.nextInt(totalCardinality);
                        byte[] key = UIO.longBytes(k);
                        keyStream.key(0, key, 0, key.length);
                        return true;
                    }, (index1, key, timestamp, tombstoned, version1, value1) -> {
                        if (value1 != null && !tombstoned) {
                            hits.incrementAndGet();
                        } else {
                            misses.incrementAndGet();
                        }
                        return true;
                    }, true);
                }
                totalReads += misses.get() + hits.get();
                readElapse = (System.currentTimeMillis() - start);
                totalReadTime += readElapse;
                readRate = (double) (misses.get() + hits.get()) * 1000 / (readElapse);
                totalHits += hits.get();
                totalMiss += misses.get();
            }

            c++;

            long reads = misses.get() + hits.get();

            LOG.set(ValueType.VALUE, "writesPerSecond", writesPerSecond);
            LOG.set(ValueType.VALUE, "writeRate", (long) writeRate);
            LOG.set(ValueType.VALUE, "writeElapse", writeElapse);
            LOG.set(ValueType.VALUE, "reads", reads);
            LOG.set(ValueType.VALUE, "readRate", (long) readRate);
            LOG.set(ValueType.VALUE, "readElapse", readElapse);
            LOG.set(ValueType.VALUE, "hits", hits.get());
            LOG.set(ValueType.VALUE, "misses", misses.get());
            LOG.set(ValueType.VALUE, "misses", misses.get());
            LOG.set(ValueType.VALUE, "mergeCount", RangeStripedCompactableIndexes.mergeCount.get());
            LOG.set(ValueType.VALUE, "splitCount", RangeStripedCompactableIndexes.splitCount.get());
            LOG.set(ValueType.VALUE, "pointTxIndexCount", LAB.pointTxIndexCount.get());
            LOG.set(ValueType.VALUE, "pointTxCalled", LAB.pointTxCalled.get());

            System.out.println(name + ":" + c
                + ", " + writesPerSecond
                + ", " + writeRate
                + ", " + writeElapse
                + ", " + reads
                + ", " + readRate
                + ", " + readElapse
                + ", " + hits.get() + ", " + misses.get()
                + ", " + RangeStripedCompactableIndexes.mergeCount.get() + ", " + RangeStripedCompactableIndexes.splitCount.get()
                + ", " + (LAB.pointTxIndexCount.get() / (double) LAB.pointTxCalled.get()));

        }

        double totalReadRate = totalReadTime > 0 ? (double) totalReads * 1000 / (totalReadTime) : 0;
        double totalWriteRate = totalWriteTime > 0 ? (double) totalWrites * 1000 / (totalWriteTime) : 0;

        AtomicLong scanCount = new AtomicLong();
        index.rowScan((index1, key, timestamp, tombstoned, version1, payload) -> {
            if (!tombstoned) {
                scanCount.incrementAndGet();
            }
            return true;
        }, true);

        String punchLine = "index:" + scanCount.get()
            + " writeMillis:" + totalWriteTime
            + " write:" + totalWrites
            + " wps:" + totalWriteRate
            + " readMillis:" + totalReadTime
            + " read:" + totalReads
            + " rps:" + totalReadRate
            + " hits:" + totalHits
            + " miss:" + totalMiss
            + " merged:" + RangeStripedCompactableIndexes.mergeCount.get()
            + " split:" + RangeStripedCompactableIndexes.splitCount
            + " readAmplification:" + (LAB.pointTxIndexCount.get() / (double) LAB.pointTxCalled.get());

        return punchLine;
    }

    public static long calculateFileSize(File file) {
        long fileSize = 0L;
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            for (File child : children) {
                fileSize += calculateFileSize(child);
            }
        } else {
            fileSize = file.length();
        }
        return fileSize;
    }
}
