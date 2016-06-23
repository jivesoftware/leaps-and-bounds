package com.jivesoftware.os.lab;

import com.google.common.io.Files;
import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.api.FormatTransformerProvider;
import com.jivesoftware.os.lab.api.Keys;
import com.jivesoftware.os.lab.api.RawEntryFormat;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.guts.Leaps;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
        LabHeapPressure labHeapPressure = new LabHeapPressure(1024 * 1024 * 10, new AtomicLong());
        LABEnvironment env = new LABEnvironment(LABEnvironment.buildLABCompactorThreadPool(4),
            LABEnvironment.buildLABDestroyThreadPool(1),
            root,
            false, labHeapPressure, 1, 2, leapsCache);

        long splitAfterSizeInBytes = 16; //1024 * 1024 * 1024;
        ValueIndex index = env.open("foo", 4096, 1024 * 1024 * 10, splitAfterSizeInBytes, -1, -1, FormatTransformerProvider.NO_OP, new LABRawhide(),
            new RawEntryFormat(0, 0));

        AtomicLong count = new AtomicLong();
        AtomicLong fails = new AtomicLong();

        while (count.get() < 100) {
            index.append((stream) -> {
                for (int i = 0; i < 10; i++) {
                    long v = count.get();
                    stream.stream(-1, UIO.longBytes(v), v, false, 0, UIO.longBytes(v));
                    count.incrementAndGet();
                }
                return true;
            }, fsync);

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
            index.commit(true);
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
                index.rangeScan(UIO.longBytes(f), UIO.longBytes(t),
                    (int index1, byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) -> {
                        boolean added = rangeScan.add(UIO.bytesLong(key));
                        //Assert.assertTrue(scanned.add(UIO.bytesLong(key)), "Already contained " + UIO.bytesLong(key));
                        if (!added) {
                            fails.incrementAndGet();
                            System.out.println("RANGE FAILED: from:" + ff + " to:" + tt + " already contained " + UIO.bytesLong(key));
                        }
                        return true;
                    });

                if (rangeScan.size() != t - f) {
                    fails.incrementAndGet();
                    System.out.print("RANGE FAILED: from:" + f + " to:" + t + " result:" + rangeScan);
                }
            }

        }

        HashSet<Long> rowScan = new HashSet<>();
        index.rowScan((int index1, byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) -> {
            boolean added = rowScan.add(UIO.bytesLong(key));
            //Assert.assertTrue(scanned.add(UIO.bytesLong(key)), "Already contained " + UIO.bytesLong(key));
            if (!added) {
                fails.incrementAndGet();
                System.out.println("RANGE FAILED: already contained " + UIO.bytesLong(key));
            }
            return true;
        });

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
        LabHeapPressure labHeapPressure = new LabHeapPressure(1024 * 1024 * 10, new AtomicLong());
        LABEnvironment env = new LABEnvironment(LABEnvironment.buildLABCompactorThreadPool(4), LABEnvironment.buildLABDestroyThreadPool(1), root,
            false, labHeapPressure, 1, 2, leapsCache);

        ValueIndex index = env.open("foo", 4096, 1024 * 1024 * 10, 16, -1, -1, FormatTransformerProvider.NO_OP, new LABRawhide(), new RawEntryFormat(0, 0));

        index.append((stream) -> {
            stream.stream(-1, UIO.longBytes(1), System.currentTimeMillis(), false, 0, UIO.longBytes(1));
            stream.stream(-1, UIO.longBytes(2), System.currentTimeMillis(), false, 0, UIO.longBytes(2));
            stream.stream(-1, UIO.longBytes(3), System.currentTimeMillis(), false, 0, UIO.longBytes(3));
            return true;
        }, fsync);
        List<Future<Object>> awaitable = index.commit(fsync);
        for (Future<Object> future : awaitable) {
            future.get();
        }

        index.append((stream) -> {
            stream.stream(-1, UIO.longBytes(7), System.currentTimeMillis(), false, 0, UIO.longBytes(7));
            stream.stream(-1, UIO.longBytes(8), System.currentTimeMillis(), false, 0, UIO.longBytes(8));
            stream.stream(-1, UIO.longBytes(9), System.currentTimeMillis(), false, 0, UIO.longBytes(9));
            return true;
        }, fsync);
        awaitable = index.commit(fsync);
        for (Future<Object> future : awaitable) {
            future.get();
        }

        Assert.assertFalse(index.isEmpty());

        index.rowScan((index1, key, timestamp, tombstoned, version, payload) -> {
            //System.out.println(Arrays.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + Arrays.toString(payload));
            return true;
        });

        long[] expected = new long[]{1, 2, 3, 7, 8, 9};
        testExpected(index, expected);
        testExpectedMultiGet(index, expected);
        testNotExpected(index, new long[]{0, 4, 5, 6, 10});
        testNotExpectedMultiGet(index, new long[]{0, 4, 5, 6, 10});
        testScanExpected(index, expected);
        testRangeScanExpected(index, UIO.longBytes(2), null, new long[]{2, 3, 7, 8, 9});
        testRangeScanExpected(index, UIO.longBytes(2), UIO.longBytes(7), new long[]{2, 3});
        testRangeScanExpected(index, UIO.longBytes(4), UIO.longBytes(7), new long[]{});

        index.commit(fsync);

        index.append((stream) -> {
            stream.stream(-1, UIO.longBytes(1), System.currentTimeMillis(), true, 1, UIO.longBytes(1));
            stream.stream(-1, UIO.longBytes(2), System.currentTimeMillis(), true, 1, UIO.longBytes(2));
            stream.stream(-1, UIO.longBytes(3), System.currentTimeMillis(), true, 1, UIO.longBytes(3));
            return true;
        }, fsync);

        expected = new long[]{7, 8, 9};
        testExpected(index, expected);
        testExpectedMultiGet(index, expected);
        testNotExpected(index, new long[]{0, 4, 5, 6, 10});
        testNotExpectedMultiGet(index, new long[]{0, 4, 5, 6, 10});
        testScanExpected(index, expected);
        testRangeScanExpected(index, UIO.longBytes(1), UIO.longBytes(9), new long[]{7, 8});

        env.shutdown();

    }

    private void testExpectedMultiGet(ValueIndex index, long[] expected) throws Exception {
        index.get((Keys.KeyStream keyStream) -> {
            for (int i = 0; i < expected.length; i++) {
                keyStream.key(i, UIO.longBytes(expected[i]), 0, 8);
            }
            return true;
        }, (int index1, byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) -> {
            Assert.assertEquals(UIO.bytesLong(payload), expected[index1]);
            return true;
        });
    }

    private void testExpected(ValueIndex index, long[] expected) throws Exception {
        for (int i = 0; i < expected.length; i++) {
            long e = expected[i];
            index.get(UIO.longBytes(expected[i]), (int index1, byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) -> {
                Assert.assertEquals(UIO.bytesLong(payload), e);
                return true;
            });
        }
    }

    private void testNotExpectedMultiGet(ValueIndex index, long[] notExpected) throws Exception {
        index.get((Keys.KeyStream keyStream) -> {
            for (long i : notExpected) {
                keyStream.key(-1, UIO.longBytes(i), 0, 8);
            }
            return true;
        }, (int index1, byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) -> {
            if (key != null || payload != null) {
                Assert.fail(Arrays.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + Arrays.toString(payload));
            }
            return true;
        });
    }

    private void testNotExpected(ValueIndex index, long[] notExpected) throws Exception {
        for (long i : notExpected) {
            index.get(UIO.longBytes(i), (int index1, byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) -> {
                if (key != null || payload != null) {
                    Assert.fail(Arrays.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + Arrays.toString(payload));
                }
                return true;
            });
        }
    }

    private void testScanExpected(ValueIndex index, long[] expected) throws Exception {
        System.out.println("Checking full scan");
        List<Long> scanned = new ArrayList<>();
        index.rowScan((int index1, byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) -> {
            System.out.println("scan:" + Arrays.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + Arrays.toString(payload));
            if (!tombstoned) {
                scanned.add(UIO.bytesLong(payload));
            }
            return true;
        });
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
            System.out.println("scan:" + Arrays.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + Arrays.toString(payload));
            if (!tombstoned) {
                scanned.add(UIO.bytesLong(payload));
            }
            return true;
        });
        Assert.assertEquals(scanned.size(), expected.length);
        for (int i = 0; i < expected.length; i++) {
            System.out.println((long) scanned.get(i) + " vs " + expected[i]);
            Assert.assertEquals((long) scanned.get(i), expected[i]);
        }
    }

}
