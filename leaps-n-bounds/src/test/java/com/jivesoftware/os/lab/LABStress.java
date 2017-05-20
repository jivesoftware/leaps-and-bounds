package com.jivesoftware.os.lab;

import com.google.common.io.Files;
import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.api.Keys;
import com.jivesoftware.os.lab.api.MemoryRawEntryFormat;
import com.jivesoftware.os.lab.api.NoOpFormatTransformerProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueIndexConfig;
import com.jivesoftware.os.lab.api.rawhide.FixedWidthRawhide;
import com.jivesoftware.os.lab.guts.LABHashIndexType;
import com.jivesoftware.os.lab.guts.Leaps;
import com.jivesoftware.os.lab.guts.RangeStripedCompactableIndexes;
import com.jivesoftware.os.lab.guts.StripingBolBufferLocks;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.io.File;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;

/**
 * @author jonathan.colt
 */
public class LABStress {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    @Test(enabled = true)
    public void stressWritesTest() throws Exception {

        LABHashIndexType indexType = LABHashIndexType.cuckoo;
        double hashIndexLoadFactor = 2d;
        File root = Files.createTempDir();
        System.out.println(root.getAbsolutePath());
        AtomicLong globalHeapCostInBytes = new AtomicLong();
        LABStats stats = new LABStats();
        ValueIndex index = createIndex(root, indexType, hashIndexLoadFactor, stats, globalHeapCostInBytes);

        int totalCardinality = 100_000_000;

        printLabels();

        String write = stress("warm:jit",
            stats,
            index,
            totalCardinality,
            800_000, // writesPerSecond
            1_000_000, //writeCount
            1, //readForNSeconds
            1_000_000, // readCount
            false,
            globalHeapCostInBytes); // removes

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

        stats = new LABStats();
        globalHeapCostInBytes = new AtomicLong();
        root = Files.createTempDir();
        index = createIndex(root, indexType, hashIndexLoadFactor, stats, globalHeapCostInBytes);

        // ---
        printLabels();

        totalCardinality = 100_000_000;

        write = stress("stress:RW",
            stats,
            index,
            totalCardinality,
            250_000, // writesPerSecond
            1_000, //writeCount
            1, //readForNSeconds
            1_000, // readCount
            false,
            globalHeapCostInBytes); // removes

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

        printLabels();

        write = stress("stress:R",
            stats,
            index,
            totalCardinality,
            0, // writesPerSecond
            0, //writeCount
            10, //readForNSeconds
            1_000, // readCount
            false,
            globalHeapCostInBytes); // removes

        System.out.println("\n\n");
        ((LAB) index).auditRanges((key) -> "" + UIO.bytesLong(key));
        System.out.println("\n\n");

        System.out.println(write);
        System.out.println("size:" + calculateFileSize(root) + "bytes " + root);
        System.out.println("\n\n");

    }

    private void printLabels() {

        System.out.println("sample, writes, writes/sec, writeElapse, reads, reads/sec, readElapse, hits, miss, merged, split, readAmplification, approxCount, "
            + "debt, open, closed, append, journaledAppend, merging, merged, spliting, splits, slabbed, allocationed, released, freed, gc, gcCommit, "
            + "pressureCommit, commit, fsyncedCommit, bytesWrittenToWAL, bytesWrittenAsIndex, bytesWrittenAsSplit, bytesWrittenAsMerge");

    }

    private ValueIndex createIndex(File root,
        LABHashIndexType indexType,
        double hashIndexLoadFactor,
        LABStats stats,
        AtomicLong globalHeapCostInBytes) throws Exception {
        System.out.println("Created root " + root);
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100_000, 8);
        LabHeapPressure labHeapPressure = new LabHeapPressure(stats,
            LABEnvironment.buildLABHeapSchedulerThreadPool(1),
            "default",
            1024 * 1024 * 20,
            1024 * 1024 * 40,
            globalHeapCostInBytes,
            LabHeapPressure.FreeHeapStrategy.mostBytesFirst);

        LABEnvironment env = new LABEnvironment(stats,
            LABEnvironment.buildLABSchedulerThreadPool(1),
            LABEnvironment.buildLABCompactorThreadPool(4), // compact
            LABEnvironment.buildLABDestroyThreadPool(1), // destroy
            null,
            root, // rootFile
            labHeapPressure,
            4, // minMergeDebt
            8, // maxMergeDebt
            leapsCache,
            new StripingBolBufferLocks(1024),
            true,
            false);

        env.register("8x8fixedWidthRawhide", new FixedWidthRawhide(8, 8));

        System.out.println("Created env");
        ValueIndex index = env.open(new ValueIndexConfig("foo",
            1024 * 4, // entriesBetweenLeaps
            1024 * 1024 * 10, // maxHeapPressureInBytes
            -1, // splitWhenKeysTotalExceedsNBytes
            -1, // splitWhenValuesTotalExceedsNBytes
            1024 * 1024 * 100, // splitWhenValuesAndKeysTotalExceedsNBytes
            NoOpFormatTransformerProvider.NAME,
            "8x8fixedWidthRawhide", //new LABRawhide(),
            MemoryRawEntryFormat.NAME,
            24,
            indexType,
            hashIndexLoadFactor, true));
        return index;
    }

    private String stress(String name,
        LABStats stats,
        ValueIndex index,
        int totalCardinality,
        int writesPerSecond,
        int writeCount,
        int readForNSeconds,
        int readCount,
        boolean removes,
        AtomicLong globalHeapCostInBytes) throws Exception {

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

        int c = 0;
        byte[] keyBytes = new byte[8];
        byte[] valuesBytes = new byte[8];
        BolBuffer rawEntryBuffer = new BolBuffer();
        BolBuffer keyBuffer = new BolBuffer();
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
                        stream.stream(-1,
                            UIO.longBytes(key, keyBytes, 0),
                            System.currentTimeMillis(),
                            (removes) ? rand.nextBoolean() : false,
                            version.incrementAndGet(),
                            UIO.longBytes(value.incrementAndGet(), valuesBytes, 0));
                    }
                    return true;
                }, true, rawEntryBuffer, keyBuffer);

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
                        UIO.longBytes(k, keyBytes, 0);
                        keyStream.key(0, keyBytes, 0, keyBytes.length);
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

            DecimalFormat formatter = new DecimalFormat("#,###.00");

            System.out.println(name + ":" + c
                + ", " + formatter.format(writesPerSecond)
                + ", " + formatter.format(writeRate)
                + ", " + formatter.format(writeElapse)
                + ", " + formatter.format(reads)
                + ", " + formatter.format(readRate)
                + ", " + formatter.format(readElapse)
                + ", " + formatter.format(hits.get())
                + ", " + formatter.format(misses.get())
                + ", " + RangeStripedCompactableIndexes.mergeCount.get()
                + ", " + RangeStripedCompactableIndexes.splitCount.get()
                + ", " + formatter.format((LAB.pointTxIndexCount.get() / (double) LAB.pointTxCalled.get()))
                + ", " + index.count()
                + ", " + stats.debt.longValue()
                + ", " + stats.open.longValue()
                + ", " + stats.closed.longValue()
                + ", " + stats.append.longValue()
                + ", " + stats.journaledAppend.longValue()
                + ", " + stats.merging.longValue()
                + ", " + stats.merged.longValue()
                + ", " + stats.spliting.longValue()
                + ", " + stats.splits.longValue()
                + ", " + stats.slabbed.longValue()
                + ", " + stats.allocationed.longValue()
                + ", " + stats.released.longValue()
                + ", " + stats.freed.longValue()
                + ", " + stats.gc.longValue()
                + ", " + stats.gcCommit.longValue()
                + ", " + stats.pressureCommit.longValue()
                + ", " + stats.commit.longValue()
                + ", " + stats.fsyncedCommit.longValue()
                + ", " + stats.bytesWrittenToWAL.longValue()
                + ", " + stats.bytesWrittenAsIndex.longValue()
                + ", " + stats.bytesWrittenAsSplit.longValue()
                + ", " + stats.bytesWrittenAsMerge.longValue()
            );


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
