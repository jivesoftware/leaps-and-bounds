package com.jivesoftware.os.lab;

import com.google.common.io.Files;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LABNGTest {

    @Test
    public void testEnv() throws Exception {

        boolean fsync = true;
        File root = Files.createTempDir();
        LABEnvironment env = new LABEnvironment(root, new LABValueMerger(), false, 1, 2);

        ValueIndex index = env.open("foo", 4096, 1000, 16, -1, -1);

        index.append((stream) -> {
            stream.stream(UIO.longBytes(1), System.currentTimeMillis(), false, 0, UIO.longBytes(1));
            stream.stream(UIO.longBytes(2), System.currentTimeMillis(), false, 0, UIO.longBytes(2));
            stream.stream(UIO.longBytes(3), System.currentTimeMillis(), false, 0, UIO.longBytes(3));
            return true;
        });
        List<Future<Object>> awaitable = index.commit(fsync);
        for (Future<Object> future : awaitable) {
            future.get();
        }

        index.append((stream) -> {
            stream.stream(UIO.longBytes(7), System.currentTimeMillis(), false, 0, UIO.longBytes(7));
            stream.stream(UIO.longBytes(8), System.currentTimeMillis(), false, 0, UIO.longBytes(8));
            stream.stream(UIO.longBytes(9), System.currentTimeMillis(), false, 0, UIO.longBytes(9));
            return true;
        });
        awaitable = index.commit(fsync);
        for (Future<Object> future : awaitable) {
            future.get();
        }

        Assert.assertFalse(index.isEmpty());

        index.rowScan((key, timestamp, tombstoned, version, payload) -> {
            System.out.println(Arrays.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + Arrays.toString(payload));
            return true;
        });

        long[] expected = new long[]{1, 2, 3, 7, 8, 9};
        testExpected(index, expected);
        testNotExpected(index, new long[]{0, 4, 5, 6, 10});
        testScanExpected(index, expected);
        testRangeScanExpected(index, UIO.longBytes(2), null, new long[]{2, 3, 7, 8, 9});
        testRangeScanExpected(index, UIO.longBytes(2), UIO.longBytes(7), new long[]{2, 3});
        testRangeScanExpected(index, UIO.longBytes(4), UIO.longBytes(7), new long[]{});

        index.commit(fsync);

        index.append((stream) -> {
            stream.stream(UIO.longBytes(1), System.currentTimeMillis(), true, 0, UIO.longBytes(1));
            stream.stream(UIO.longBytes(2), System.currentTimeMillis(), true, 0, UIO.longBytes(2));
            stream.stream(UIO.longBytes(3), System.currentTimeMillis(), true, 0, UIO.longBytes(3));
            return true;
        });

        expected = new long[]{7, 8, 9};
        testExpected(index, expected);
        testNotExpected(index, new long[]{0, 4, 5, 6, 10});
        testScanExpected(index, expected);
        testRangeScanExpected(index, UIO.longBytes(1), UIO.longBytes(9), new long[]{7, 8});

        env.shutdown();

    }

    private void testExpected(ValueIndex index, long[] expected) throws Exception {
        for (long i : expected) {
            long e = i;
            index.get(UIO.longBytes(i), (byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) -> {
                Assert.assertEquals(UIO.bytesLong(payload), e);
                return true;
            });
        }
    }

    private void testNotExpected(ValueIndex index, long[] notExpected) throws Exception {
        for (long i : notExpected) {
            index.get(UIO.longBytes(i), (byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) -> {
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
        index.rowScan((byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) -> {
            System.out.println("scan:" + Arrays.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + Arrays.toString(payload));
            if (payload != null) {
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
        index.rangeScan(from, to, (key, timestamp, tombstoned, version, payload) -> {
            System.out.println("scan:" + Arrays.toString(key) + " " + timestamp + " " + tombstoned + " " + version + " " + Arrays.toString(payload));
            if (payload != null) {
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
