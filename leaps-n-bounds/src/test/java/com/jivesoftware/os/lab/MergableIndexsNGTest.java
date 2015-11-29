package com.jivesoftware.os.lab;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.lab.api.GetRaw;
import com.jivesoftware.os.lab.api.NextRawEntry;
import com.jivesoftware.os.lab.api.RawEntryStream;
import com.jivesoftware.os.lab.api.ReadIndex;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.File;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class MergableIndexsNGTest {

    @Test(enabled = false)
    public void testConcurrentMerges() throws Exception {

        ExecutorService destroy = Executors.newSingleThreadExecutor();
        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        int count = 3;
        int step = 100;
        int indexes = 40;

        MergeableIndexes indexs = new MergeableIndexes();
        long time = System.currentTimeMillis();
        System.out.println("Seed:" + time);
        Random rand = new Random(1446914103456L);

        Executors.newSingleThreadExecutor().submit(() -> {
            for (int wi = 0; wi < indexes; wi++) {

                File indexFiler = File.createTempFile("a-index-" + wi, ".tmp");
                IndexFile indexFile = new IndexFile(indexFiler.getAbsolutePath(), "rw", false);
                IndexRangeId indexRangeId = new IndexRangeId(wi, wi);

                WriteLeapsAndBoundsIndex write = new WriteLeapsAndBoundsIndex(indexRangeId, indexFile, 64, 2);
                IndexTestUtils.append(rand, write, 0, step, count, desired);
                write.close();

                indexFile = new IndexFile(indexFiler.getAbsolutePath(), "r", false);
                indexs.append(new LeapsAndBoundsIndex(destroy, indexRangeId, indexFile));

            }
            Thread.sleep(10);
            return null;
        });

        MergeableIndexes.Reader reader = indexs.reader();
        assertions(reader, count, step, desired);

        File indexFiler = File.createTempFile("a-index-merged", ".tmp");
        MergeableIndexes.Merger merger = indexs.merge((id, worstCaseCount) -> {
            int updatesBetweenLeaps = 2;
            int maxLeaps = IndexUtil.calculateIdealMaxLeaps(worstCaseCount, updatesBetweenLeaps);
            return new WriteLeapsAndBoundsIndex(id, new IndexFile(indexFiler.getAbsolutePath(), "rw", false), maxLeaps, updatesBetweenLeaps);
        }, (id, index) -> {
            return new LeapsAndBoundsIndex(destroy, id, new IndexFile(indexFiler.getAbsolutePath(), "r", false));
        });
        if (merger != null) {
            merger.call();
        }

        assertions(reader, count, step, desired);
    }

    @Test(enabled = true)
    public void testTx() throws Exception {

        ExecutorService destroy = Executors.newSingleThreadExecutor();
        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        int count = 3;
        int step = 100;
        int indexes = 4;

        MergeableIndexes indexs = new MergeableIndexes();
        long time = System.currentTimeMillis();
        System.out.println("Seed:" + time);
        Random rand = new Random(1446914103456L);
        for (int wi = 0; wi < indexes; wi++) {

            File indexFiler = File.createTempFile("MergableIndexsNGTest" + File.separator + "MergableIndexsNGTest-testTx" + File.separator + "a-index-" + wi,
                ".tmp");
            IndexFile indexFile = new IndexFile(indexFiler.getAbsolutePath(), "rw", false);
            IndexRangeId indexRangeId = new IndexRangeId(wi, wi);

            WriteLeapsAndBoundsIndex write = new WriteLeapsAndBoundsIndex(indexRangeId, indexFile, 64, 2);
            IndexTestUtils.append(rand, write, 0, step, count, desired);
            write.close();

            indexFile = new IndexFile(indexFiler.getAbsolutePath(), "r", false);
            indexs.append(new LeapsAndBoundsIndex(destroy, indexRangeId, indexFile));
        }

        MergeableIndexes.Reader reader = indexs.reader();
        assertions(reader, count, step, desired);

        File indexFiler = File.createTempFile("a-index-merged", ".tmp");
        MergeableIndexes.Merger merger = indexs.merge((id, worstCaseCount) -> {
            int updatesBetweenLeaps = 2;
            int maxLeaps = IndexUtil.calculateIdealMaxLeaps(worstCaseCount, updatesBetweenLeaps);
            return new WriteLeapsAndBoundsIndex(id, new IndexFile(indexFiler.getAbsolutePath(), "rw", false), maxLeaps, updatesBetweenLeaps);
        }, (id, index) -> {
            return new LeapsAndBoundsIndex(destroy, id, new IndexFile(indexFiler.getAbsolutePath(), "r", false));
        });
        merger.call();

        assertions(reader, count, step, desired);
    }

    private void assertions(MergeableIndexes.Reader reader,
        int count, int step,
        ConcurrentSkipListMap<byte[], byte[]> desired) throws
        Exception {

        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];
        ReadIndex[] acquired = reader.acquire(1024);
        NextRawEntry rowScan = IndexUtil.rowScan(acquired);
        RawEntryStream stream = (rawEntry, offset, length) -> {
            System.out.println("Expected:key:" + UIO.bytesLong(keys.get(index[0])) + " Found:" + SimpleRawEntry.toString(rawEntry));
            Assert.assertEquals(UIO.bytesLong(keys.get(index[0])), SimpleRawEntry.key(rawEntry));
            index[0]++;
            return true;
        };
        while (rowScan.next(stream));
        Assert.assertEquals(index[0], keys.size());
        reader.release();
        System.out.println("rowScan PASSED");

        acquired = reader.acquire(1024);
        for (int i = 0; i < count * step; i++) {
            long k = i;
            GetRaw getPointer = IndexUtil.get(acquired);
            byte[] key = UIO.longBytes(k);
            stream = (rawEntry, offset, length) -> {
                System.out.println("->" + SimpleRawEntry.key(rawEntry) + " " + SimpleRawEntry.value(rawEntry) + " " + offset + " " + length);
                if (rawEntry != null) {
                    System.out.println("Got: " + SimpleRawEntry.toString(rawEntry));
                    byte[] rawKey = UIO.longBytes(SimpleRawEntry.key(rawEntry));
                    Assert.assertEquals(rawKey, key);
                    byte[] d = desired.get(key);
                    if (d == null) {
                        Assert.fail();
                    } else {
                        Assert.assertEquals(SimpleRawEntry.value(rawEntry), SimpleRawEntry.value(d));
                    }
                } else {
                    Assert.assertFalse(desired.containsKey(key), "Desired doesn't contain:" + UIO.bytesLong(key));
                }
                return rawEntry != null;
            };
            getPointer.get(key, stream);
        }
        reader.release();
        System.out.println("getPointer PASSED");

        acquired = reader.acquire(1024);
        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;

            int[] streamed = new int[1];
            stream = (rawEntry, offset, length) -> {
                if (SimpleRawEntry.value(rawEntry) > -1) {
                    System.out.println("Streamed:" + SimpleRawEntry.toString(rawEntry));
                    streamed[0]++;
                }
                return true;
            };

            System.out.println("Asked index:" + _i + " key:" + UIO.bytesLong(keys.get(_i)) + " to:" + UIO.bytesLong(keys.get(_i + 3)));
            NextRawEntry rangeScan = IndexUtil.rangeScan(acquired, keys.get(_i), keys.get(_i + 3));
            while (rangeScan.next(stream));
            Assert.assertEquals(3, streamed[0]);

        }
        reader.release();
        System.out.println("rangeScan PASSED");

        acquired = reader.acquire(1024);
        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;
            int[] streamed = new int[1];
            stream = (rawEntry, offset, length) -> {
                if (SimpleRawEntry.value(rawEntry) > -1) {
                    streamed[0]++;
                }
                return true;
            };
            NextRawEntry rangeScan = IndexUtil.rangeScan(acquired, UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1), keys.get(_i + 3));
            while (rangeScan.next(stream));
            Assert.assertEquals(2, streamed[0]);

        }
        reader.release();
        System.out.println("rangeScan2 PASSED");
    }

}
