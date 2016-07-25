package com.jivesoftware.os.lab;

import com.google.common.io.Files;
import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.api.FixedWidthRawhide;
import com.jivesoftware.os.lab.api.FormatTransformerProvider;
import com.jivesoftware.os.lab.api.Keys;
import com.jivesoftware.os.lab.api.RawEntryFormat;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.guts.Leaps;
import com.jivesoftware.os.lab.guts.RangeStripedCompactableIndexes;
import com.jivesoftware.os.lab.io.api.UIO;
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
public class LABStressWrites {

    @Test(enabled = false)
    public void stressWrites() throws Exception {

        File root = Files.createTempDir();
        ValueIndex index = createIndex(root);

        boolean writeMonotonicly = false;

        System.out.println("Sample, Writes, Writes/Sec, Reads, Reads/Sec, Hits, Miss, Merged, Split, ReadAmplification");

        int iterations = 1000;
        int batchSize = 500;
        int totalCardinality = 500_000; // iterations * batchSize * 2;

        String write = stress("warm:jit",
            index,
            totalCardinality,
            iterations, // writeCount
            batchSize, //writeBatchSize
            writeMonotonicly, // writeMonotonicly
            batchSize, //getCount
            false); // removes
        List<Future<Object>> futures = index.commit(true);
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
        System.out.println("Sample, Writes, Writes/Sec, Reads, Reads/Sec, Hits, Miss, Merged, Split, ReadAmplification");

        iterations = 1000;
        batchSize = 50000;
        totalCardinality = 500_000; //iterations * batchSize * 2;

        write = stress("stress:RW",
            index,
            totalCardinality,
            iterations, // iterations
            batchSize, //writeBatchSize
            writeMonotonicly, // writeMonotonicly
            batchSize, //getCount
            false); // removes

        System.out.println("\n\n");
        ((LAB) index).auditRanges((key) -> "" + UIO.bytesLong(key));
        System.out.println("\n\n");

        System.out.println(write);
        System.out.println("size:" + calculateFileSize(root) + "bytes " + root);
        System.out.println("\n\n");

        System.out.println("COMMIT ALL");
        futures = index.commit(true);
        for (Future<Object> future : (futures != null) ? futures : Collections.<Future<Object>>emptyList()) {
            future.get();
        }
        System.out.println("COMMITED ALL");

        System.out.println("COMPACT ALL");
        futures = index.compact(true, 0, 0);
        for (Future<Object> future : (futures != null) ? futures : Collections.<Future<Object>>emptyList()) {
            future.get();
        }
        System.out.println("COMPACTED ALL");

        write = stress("stress:R",
            index,
            totalCardinality,
            iterations, // iterations
            0, //writeBatchSize
            writeMonotonicly, // writeMonotonicly
            batchSize, //getCount
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
        LabHeapPressure labHeapPressure = new LabHeapPressure(1024 * 1024 * 10, new AtomicLong());
        LABEnvironment env = new LABEnvironment(LABEnvironment.buildLABCompactorThreadPool(4), // compact
            LABEnvironment.buildLABDestroyThreadPool(1), // destroy
            root, // rootFile
            true, // useMemMap
            labHeapPressure,
            4, // minMergeDebt
            8, // maxMergeDebt
            leapsCache);
        System.out.println("Created env");
        ValueIndex index = env.open("foo",
            2048, // entriesBetweenLeaps
            1024 * 1024 * 512, // maxHeapPressureInBytes
            -1, // splitWhenKeysTotalExceedsNBytes
            -1, // splitWhenValuesTotalExceedsNBytes
            1024 * 1024 * 10, // splitWhenValuesAndKeysTotalExceedsNBytes
            FormatTransformerProvider.NO_OP,
            new FixedWidthRawhide(8, 8), //new LABRawhide(),
            new RawEntryFormat(0, 0));
        return index;
    }

    private String stress(String name,
        ValueIndex index,
        int totalCardinality,
        int writeCount,
        int writeBatchSize,
        boolean writeMonotonicly,
        int readCount,
        boolean removes) throws Exception {

        AtomicLong version = new AtomicLong();
        AtomicLong value = new AtomicLong();
        AtomicLong count = new AtomicLong();

        long totalReadTime = 0;
        long totalReads = 0;
        long totalWriteTime = 0;
        long totalWrites = 0;

        long totalHits = 0;
        long totalMiss = 0;

        Random rand = new Random(12345);
        AtomicLong monotonic = new AtomicLong();

        for (int c = 0; c < writeCount; c++) {
            long start = System.currentTimeMillis();
            long writeElapse = 0;
            double writeRate = 0;
            if (writeBatchSize > 0) {
                index.append((ValueStream stream) -> {
                    for (int i = 0; i < writeBatchSize; i++) {
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
                totalWrites += writeBatchSize;
                //System.out.println("Commit Elapse:" + (System.currentTimeMillis() - start));
                writeElapse = (System.currentTimeMillis() - start);
                totalWriteTime += writeElapse;
                writeRate = (double) writeBatchSize * 1000 / (writeElapse);
            } else {
                monotonic.addAndGet(readCount);
            }

            start = System.currentTimeMillis();
            AtomicLong hits = new AtomicLong();
            long readElapse = 0;
            double readRate = 0;
            if (readCount > 0) {
                long m = monotonic.get();
                LAB.pointTxCalled.set(0);
                LAB.pointTxIndexCount.set(0);
                for (int i = 0; i < readCount; i++) {

//                    index.get(UIO.longBytes(rand.nextInt(totalCardinality)), (index1, key, timestamp, tombstoned, version1, value1) -> {
//                        if (value1 != null && !tombstoned) {
//                            hits.incrementAndGet();
//                        }
//                        return true;
//                    });
                    int ii = i;
                    index.get((Keys.KeyStream keyStream) -> {
                        long k = 0;
                        if (writeMonotonicly) {
                            k = Math.max(0, (m - readCount) + ii);
                        } else {
                            k = rand.nextInt(totalCardinality);
                        }
                        byte[] key = UIO.longBytes(k);
                        keyStream.key(0, key, 0, key.length);
                        return true;
                    }, (index1, key, timestamp, tombstoned, version1, value1) -> {
                        if (value1 != null && !tombstoned) {
                            hits.incrementAndGet();
                        }
                        return true;
                    });
                }
                totalReads += readCount;
                readElapse = (System.currentTimeMillis() - start);
                totalReadTime += readElapse;
                readRate = (double) readCount * 1000 / (readElapse);
                totalHits += hits.get();
                totalMiss += (readCount - hits.get());
            }

            System.out.println(name + ":" + c + ", " + count.get() + ", " + writeRate + ", " + readCount + ", " + readRate + ", " + hits
                .get() + ", " + (readCount - hits.get()) + ", " + RangeStripedCompactableIndexes.mergeCount.get() + ", " + RangeStripedCompactableIndexes.splitCount
                .get() + ", " + (LAB.pointTxIndexCount.get() / (double) LAB.pointTxCalled.get()));

        }

        double totalReadRate = totalReadTime > 0 ? (double) totalReads * 1000 / (totalReadTime) : 0;
        double totalWriteRate = totalWriteTime > 0 ? (double) totalWrites * 1000 / (totalWriteTime) : 0;

        AtomicLong scanCount = new AtomicLong();
        index.rowScan((int index1, byte[] key, long timestamp, boolean tombstoned, long version1, byte[] payload) -> {
            if (!tombstoned) {
                scanCount.incrementAndGet();
            }
            return true;
        });

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
