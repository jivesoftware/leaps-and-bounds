package com.jivesoftware.os.lab;

import com.google.common.io.Files;
import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.api.Keys;
import com.jivesoftware.os.lab.api.MemoryRawEntryFormat;
import com.jivesoftware.os.lab.api.NoOpFormatTransformerProvider;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueIndexConfig;
import com.jivesoftware.os.lab.api.rawhide.LABRawhide;
import com.jivesoftware.os.lab.guts.IndexUtil;
import com.jivesoftware.os.lab.guts.Leaps;
import com.jivesoftware.os.lab.guts.StripingBolBufferLocks;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LABNGTest {

    @Test
    public void testRangeScanInsane() throws Exception {

        boolean fsync = true;
        File root = Files.createTempDir();
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        LabHeapPressure labHeapPressure = new LabHeapPressure(new LABStats(),
            LABEnvironment.buildLABHeapSchedulerThreadPool(1),
            "default",
            1024 * 1024 * 10,
            1024 * 1024 * 10,
            new AtomicLong());
        LABEnvironment env = new LABEnvironment(new LABStats(),
            LABEnvironment.buildLABSchedulerThreadPool(1),
            LABEnvironment.buildLABCompactorThreadPool(4),
            LABEnvironment.buildLABDestroyThreadPool(1),
            "wal", 1024 * 1024 * 10,
            1000, 1024 * 1024 * 10,
            1024 * 1024 * 10, root,
            labHeapPressure, 1, 2, leapsCache,
            new StripingBolBufferLocks(1024),
            true);

        long splitAfterSizeInBytes = 16; //1024 * 1024 * 1024;

        ValueIndexConfig valueIndexConfig = new ValueIndexConfig("foo", 4096, 1024 * 1024 * 10, splitAfterSizeInBytes, -1, -1,
            NoOpFormatTransformerProvider.NAME, LABRawhide.NAME, MemoryRawEntryFormat.NAME);

        ValueIndex index = env.open(valueIndexConfig);

        AtomicLong count = new AtomicLong();
        AtomicLong fails = new AtomicLong();

        BolBuffer rawEntryBuffer = new BolBuffer();
        BolBuffer keyBuffer = new BolBuffer();
        while (count.get() < 100) {
            index.append((stream) -> {
                for (int i = 0; i < 10; i++) {
                    long v = count.get();
                    stream.stream(-1, UIO.longBytes(v, new byte[8], 0), v, false, 0, UIO.longBytes(v, new byte[8], 0));
                    count.incrementAndGet();
                }
                return true;
            }, fsync, rawEntryBuffer, keyBuffer);

            long c = count.get();
            AtomicLong f;
            do {
                f = new AtomicLong();
                assertRangeScan(c, index, f);
                if (f.get() > 0) {
                    System.out.println("SPINNING ");
                }
                fails.addAndGet(f.get());
            } while (f.get() > 0);
            index.commit(true, true);
            do {
                f = new AtomicLong();
                assertRangeScan(c, index, f);
                if (f.get() > 0) {
                    System.out.println("SPINNING");
                }
                fails.addAndGet(f.get());
            } while (f.get() > 0);
            System.out.println(c + " -------------------------------------");
        }

        System.out.println("fails:" + fails.get());
        Assert.assertEquals(fails.get(), 0);
    }

    private void assertRangeScan(long c, ValueIndex index, AtomicLong fails) throws Exception {

        for (long f = 0; f < c; f++) {

            for (long t = f; t < c; t++) {

                long ff = f;
                long tt = t;

                HashSet<Long> rangeScan = new HashSet<>();
                index.rangeScan(UIO.longBytes(f, new byte[8], 0), UIO.longBytes(t, new byte[8], 0),
                    (index1, key, timestamp, tombstoned, version, payload) -> {
                        boolean added = rangeScan.add(key.getLong(0));
                        //Assert.assertTrue(scanned.add(UIO.bytesLong(key)), "Already contained " + UIO.bytesLong(key));
                        if (!added) {
                            fails.incrementAndGet();
                            System.out.println("RANGE FAILED: from:" + ff + " to:" + tt + " already contained " + key.getLong(0));
                        }
                        return true;
                    }, true);

                if (rangeScan.size() != t - f) {
                    fails.incrementAndGet();
                    System.out.print("RANGE FAILED: from:" + f + " to:" + t + " result:" + rangeScan);
                }
            }

        }

        HashSet<Long> rowScan = new HashSet<>();
        index.rowScan((index1, key, timestamp, tombstoned, version, payload) -> {
            boolean added = rowScan.add(key.getLong(0));
            //Assert.assertTrue(scanned.add(UIO.bytesLong(key)), "Already contained " + UIO.bytesLong(key));
            if (!added) {
                fails.incrementAndGet();
                System.out.println("RANGE FAILED: already contained " + key.getLong(0));
            }
            return true;
        }, true);

        if (rowScan.size() != c) {
            fails.incrementAndGet();
            System.out.print("ROW FAILED: expected " + c + " result:" + rowScan);
        }
    }

    @Test
    public void testEnv() throws Exception {

        boolean fsync = true;
        File root = Files.createTempDir();
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        LabHeapPressure labHeapPressure = new LabHeapPressure(new LABStats(),
            LABEnvironment.buildLABHeapSchedulerThreadPool(1),
            "default",
            1024 * 1024 * 10,
            1024 * 1024 * 10,
            new AtomicLong());
        LABEnvironment env = new LABEnvironment(
            new LABStats(),
            LABEnvironment.buildLABSchedulerThreadPool(1),
            LABEnvironment.buildLABCompactorThreadPool(4),
            LABEnvironment.buildLABDestroyThreadPool(1),
            "wal", 1024 * 1024 * 10,
            1000, 1024 * 1024 * 10,
            1024 * 1024 * 10, root,
            labHeapPressure, 1, 2, leapsCache,
            new StripingBolBufferLocks(1024),
            true);

        ValueIndexConfig valueIndexConfig = new ValueIndexConfig("foo", 4096, 1024 * 1024 * 10, 16, -1, -1,
            NoOpFormatTransformerProvider.NAME, LABRawhide.NAME, MemoryRawEntryFormat.NAME);

        ValueIndex index = env.open(valueIndexConfig);
        BolBuffer rawEntryBuffer = new BolBuffer();
        BolBuffer keyBuffer = new BolBuffer();
        index.append((stream) -> {
            stream.stream(-1, UIO.longBytes(1, new byte[8], 0), System.currentTimeMillis(), false, 0, UIO.longBytes(1, new byte[8], 0));
            stream.stream(-1, UIO.longBytes(2, new byte[8], 0), System.currentTimeMillis(), false, 0, UIO.longBytes(2, new byte[8], 0));
            stream.stream(-1, UIO.longBytes(3, new byte[8], 0), System.currentTimeMillis(), false, 0, UIO.longBytes(3, new byte[8], 0));
            return true;
        }, fsync, rawEntryBuffer, keyBuffer);
        commitAndWait(index, fsync);

        index.append((stream) -> {
            stream.stream(-1, UIO.longBytes(7, new byte[8], 0), System.currentTimeMillis(), false, 0, UIO.longBytes(7, new byte[8], 0));
            stream.stream(-1, UIO.longBytes(8, new byte[8], 0), System.currentTimeMillis(), false, 0, UIO.longBytes(8, new byte[8], 0));
            stream.stream(-1, UIO.longBytes(9, new byte[8], 0), System.currentTimeMillis(), false, 0, UIO.longBytes(9, new byte[8], 0));
            return true;
        }, fsync, rawEntryBuffer, keyBuffer);
        commitAndWait(index, fsync);

        Assert.assertFalse(index.isEmpty());

        index.rowScan((index1, key, timestamp, tombstoned, version, payload) -> {
            //System.out.println(Arrays.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + Arrays.toString(payload));
            return true;
        }, true);

        long[] expected = new long[]{1, 2, 3, 7, 8, 9};
        testExpected(index, expected);
        testExpectedMultiGet(index, expected);
        testNotExpected(index, new long[]{0, 4, 5, 6, 10});
        testNotExpectedMultiGet(index, new long[]{0, 4, 5, 6, 10});
        testScanExpected(index, expected);
        testRangeScanExpected(index, UIO.longBytes(2, new byte[8], 0), null, new long[]{2, 3, 7, 8, 9});
        testRangeScanExpected(index, UIO.longBytes(2, new byte[8], 0), UIO.longBytes(7, new byte[8], 0), new long[]{2, 3});
        testRangeScanExpected(index, UIO.longBytes(4, new byte[8], 0), UIO.longBytes(7, new byte[8], 0), new long[]{});

        index.commit(fsync, true);

        index.append((stream) -> {
            stream.stream(-1, UIO.longBytes(1, new byte[8], 0), System.currentTimeMillis(), true, 1, UIO.longBytes(1, new byte[8], 0));
            stream.stream(-1, UIO.longBytes(2, new byte[8], 0), System.currentTimeMillis(), true, 1, UIO.longBytes(2, new byte[8], 0));
            stream.stream(-1, UIO.longBytes(3, new byte[8], 0), System.currentTimeMillis(), true, 1, UIO.longBytes(3, new byte[8], 0));
            return true;
        }, fsync, rawEntryBuffer, keyBuffer);

        expected = new long[]{7, 8, 9};
        testExpected(index, expected);
        testExpectedMultiGet(index, expected);
        testNotExpected(index, new long[]{0, 4, 5, 6, 10});
        testNotExpectedMultiGet(index, new long[]{0, 4, 5, 6, 10});
        testScanExpected(index, expected);
        testRangeScanExpected(index, UIO.longBytes(1, new byte[8], 0), UIO.longBytes(9, new byte[8], 0), new long[]{7, 8});

        env.shutdown();

    }

    @Test
    public void testClobber() throws Exception {

        boolean fsync = true;
        File root = Files.createTempDir();
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        LabHeapPressure labHeapPressure = new LabHeapPressure(new LABStats(),
            LABEnvironment.buildLABHeapSchedulerThreadPool(1),
            "default",
            1024 * 1024 * 10,
            1024 * 1024 * 10,
            new AtomicLong());
        LABEnvironment env = new LABEnvironment(new LABStats(),
            LABEnvironment.buildLABSchedulerThreadPool(1),
            LABEnvironment.buildLABCompactorThreadPool(4),
            LABEnvironment.buildLABDestroyThreadPool(1),
            "wal", 1024 * 1024 * 10,
            1000, 1024 * 1024 * 10,
            1024 * 1024 * 10, root,
            labHeapPressure, 1, 2, leapsCache,
            new StripingBolBufferLocks(1024),
            true);

        ValueIndexConfig valueIndexConfig = new ValueIndexConfig("foo", 4096, 1024 * 1024 * 10, 16, -1, -1,
            NoOpFormatTransformerProvider.NAME, LABRawhide.NAME, MemoryRawEntryFormat.NAME);

        ValueIndex index = env.open(valueIndexConfig);
        BolBuffer rawEntryBuffer = new BolBuffer();
        BolBuffer keyBuffer = new BolBuffer();

        index.append((stream) -> {
            stream.stream(-1, UIO.longBytes(1, new byte[8], 0), 1, false, 0, UIO.longBytes(1, new byte[8], 0));
            return true;
        }, fsync, rawEntryBuffer, keyBuffer);

        commitAndWait(index, fsync);

        index.append((stream) -> {
            stream.stream(-1, UIO.longBytes(1, new byte[8], 0), 4, false, 0, UIO.longBytes(7, new byte[8], 0));
            return true;
        }, fsync, rawEntryBuffer, keyBuffer);
        commitAndWait(index, fsync);

        index.append((stream) -> {
            stream.stream(-1, UIO.longBytes(1, new byte[8], 0), 1, false, 0, UIO.longBytes(1, new byte[8], 0));
            return true;
        }, fsync, rawEntryBuffer, keyBuffer);

        commitAndWait(index, fsync);

        Assert.assertFalse(index.isEmpty());

        long[] expectedValues = new long[]{-1, 7};

        index.get((keyStream) -> {
            for (int i = 1; i < 2; i++) {
                keyStream.key(i, UIO.longBytes(i), 0, 8);
            }
            return true;
        }, (index1, key, timestamp, tombstoned, version, payload) -> {
            System.out.println(IndexUtil.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + IndexUtil.toString(payload));
            Assert.assertEquals(UIO.bytesLong(payload.copy()), expectedValues[index1]);
            return true;
        }, true);

        env.shutdown();

    }

    private void commitAndWait(ValueIndex index, boolean fsync) throws Exception, ExecutionException, InterruptedException {
        List<Future<Object>> awaitable = index.commit(fsync, true);
        for (Future<Object> future : awaitable) {
            future.get();
        }
    }

    private void testExpectedMultiGet(ValueIndex index, long[] expected) throws Exception {
        index.get((Keys.KeyStream keyStream) -> {
            for (int i = 0; i < expected.length; i++) {
                keyStream.key(i, UIO.longBytes(expected[i], new byte[8], 0), 0, 8);
            }
            return true;
        }, (index1, key, timestamp, tombstoned, version, payload) -> {
            Assert.assertEquals(payload.getLong(0), expected[index1]);
            return true;
        }, true);
    }

    private void testExpected(ValueIndex index, long[] expected) throws Exception {
        for (int i = 0; i < expected.length; i++) {
            long e = expected[i];
            int ii = i;
            index.get(
                (keyStream) -> {
                    byte[] key = UIO.longBytes(expected[ii], new byte[8], 0);
                    keyStream.key(0, key, 0, key.length);
                    return true;
                },
                (index1, key, timestamp, tombstoned, version, payload) -> {
                    Assert.assertEquals(payload.getLong(0), e);
                    return true;
                }, true);
        }
    }

    private void testNotExpectedMultiGet(ValueIndex index, long[] notExpected) throws Exception {
        index.get((Keys.KeyStream keyStream) -> {
            for (long i : notExpected) {
                keyStream.key(-1, UIO.longBytes(i, new byte[8], 0), 0, 8);
            }
            return true;
        }, (index1, key, timestamp, tombstoned, version, payload) -> {
            if (key != null || payload != null) {
                Assert.fail(IndexUtil.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + IndexUtil.toString(payload));
            }
            return true;
        }, true);
    }

    private void testNotExpected(ValueIndex index, long[] notExpected) throws Exception {
        for (long i : notExpected) {
            long ii = i;
            index.get(
                (keyStream) -> {
                    byte[] key = UIO.longBytes(ii, new byte[8], 0);
                    keyStream.key(0, key, 0, key.length);
                    return true;
                },
                (index1, key, timestamp, tombstoned, version, payload) -> {
                    if (key != null || payload != null) {
                        Assert.fail(IndexUtil.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + IndexUtil.toString(payload));
                    }
                    return true;
                }, true);
        }
    }

    private void testScanExpected(ValueIndex index, long[] expected) throws Exception {
        System.out.println("Checking full scan");
        List<Long> scanned = new ArrayList<>();
        index.rowScan((index1, key, timestamp, tombstoned, version, payload) -> {
            System.out.println("scan:" + IndexUtil.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + IndexUtil.toString(payload));
            if (!tombstoned) {
                scanned.add(payload.getLong(0));
            }
            return true;
        }, true);
        Assert.assertEquals(scanned.size(), expected.length);
        for (int i = 0; i < expected.length; i++) {
            System.out.println((long) scanned.get(i) + " vs " + expected[i]);
            Assert.assertEquals((long) scanned.get(i), expected[i]);
        }
    }

    private void testRangeScanExpected(ValueIndex index, byte[] from, byte[] to, long[] expected) throws Exception {

        System.out.println("Checking range scan:" + Arrays.toString(from) + "->" + Arrays.toString(to));
        List<Long> scanned = new ArrayList<>();
        index.rangeScan(from, to, (index1, key, timestamp, tombstoned, version, payload) -> {
            System.out.println("scan:" + IndexUtil.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + IndexUtil.toString(payload));
            if (!tombstoned) {
                scanned.add(payload.getLong(0));
            }
            return true;
        }, true);
        Assert.assertEquals(scanned.size(), expected.length);
        for (int i = 0; i < expected.length; i++) {
            System.out.println((long) scanned.get(i) + " vs " + expected[i]);
            Assert.assertEquals((long) scanned.get(i), expected[i]);
        }
    }

}
