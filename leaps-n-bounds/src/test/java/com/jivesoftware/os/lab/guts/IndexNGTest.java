package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.api.RawEntryFormat;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.NextRawEntry;
import com.jivesoftware.os.lab.guts.api.RawConcurrentReadableIndex;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.File;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class IndexNGTest {

    private final SimpleRawhide simpleRawEntry = new SimpleRawhide();

    @Test(enabled = true)
    public void testLeapDisk() throws Exception {

        ExecutorService destroy = Executors.newSingleThreadExecutor();
        File indexFiler = File.createTempFile("l-index", ".tmp");

        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(simpleRawEntry);

        int count = 100;
        int step = 10;

        IndexRangeId indexRangeId = new IndexRangeId(1, 1, 0);

        LABAppendableIndex write = new LABAppendableIndex(indexRangeId, new IndexFile(indexFiler, "rw", false),
            64, 10, simpleRawEntry, new RawEntryFormat(0, 0));

        IndexTestUtils.append(new Random(), write, 0, step, count, desired);
        write.closeAppendable(false);

        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        assertions(new LeapsAndBoundsIndex(destroy, indexRangeId, new IndexFile(indexFiler, "r", false), simpleRawEntry, leapsCache), count, step, desired);
    }

    @Test(enabled = false)
    public void testMemory() throws Exception {

        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(simpleRawEntry);

        int count = 10;
        int step = 10;

        ExecutorService destroy = Executors.newSingleThreadExecutor();
        RawMemoryIndex walIndex = new RawMemoryIndex(destroy, new AtomicLong(), new SimpleRawhide());

        IndexTestUtils.append(new Random(), walIndex, 0, step, count, desired);
        assertions(walIndex, count, step, desired);
    }

    @Test(enabled = false)
    public void testMemoryToDisk() throws Exception {

        ExecutorService destroy = Executors.newSingleThreadExecutor();
        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(simpleRawEntry);

        int count = 10;
        int step = 10;

        RawMemoryIndex memoryIndex = new RawMemoryIndex(destroy, new AtomicLong(), new SimpleRawhide());

        IndexTestUtils.append(new Random(), memoryIndex, 0, step, count, desired);
        assertions(memoryIndex, count, step, desired);

        File indexFiler = File.createTempFile("c-index", ".tmp");
        IndexRangeId indexRangeId = new IndexRangeId(1, 1, 0);
        LABAppendableIndex disIndex = new LABAppendableIndex(indexRangeId, new IndexFile(indexFiler, "rw", false),
            64, 10, simpleRawEntry, new RawEntryFormat(0, 0));

        disIndex.append(memoryIndex);
        disIndex.closeAppendable(false);

        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        assertions(new LeapsAndBoundsIndex(destroy, indexRangeId, new IndexFile(indexFiler, "r", false), simpleRawEntry, leapsCache), count, step, desired);

    }

    private void assertions(RawConcurrentReadableIndex walIndex, int count, int step, ConcurrentSkipListMap<byte[], byte[]> desired) throws
        Exception {
        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];
        ReadIndex reader = walIndex.acquireReader();
        try {
            NextRawEntry rowScan = reader.rowScan();
            RawEntryStream stream = (rawEntryFormat, rawEntry, offset, length) -> {
                System.out.println("rowScan:" + SimpleRawhide.key(rawEntry));
                Assert.assertEquals(UIO.bytesLong(keys.get(index[0])), SimpleRawhide.key(rawEntry));
                index[0]++;
                return true;
            };
            while (rowScan.next(stream) == NextRawEntry.Next.more);
        } finally {
            reader.release();
            reader = null;
        }
        System.out.println("Point Get");
        for (int i = 0; i < count * step; i++) {
            long k = i;
            reader = walIndex.acquireReader();
            try {
                GetRaw getRaw = reader.get();
                byte[] key = UIO.longBytes(k);
                RawEntryStream stream = (rawEntryFormat, rawEntry, offset, length) -> {

                    System.out.println("Got: " + SimpleRawhide.toString(rawEntry));
                    if (rawEntry != null) {
                        byte[] rawKey = UIO.longBytes(SimpleRawhide.key(rawEntry));
                        Assert.assertEquals(rawKey, key);
                        byte[] d = desired.get(key);
                        if (d == null) {
                            Assert.fail();
                        } else {
                            Assert.assertEquals(SimpleRawhide.value(rawEntry), SimpleRawhide.value(d));
                        }
                    } else {
                        Assert.assertFalse(desired.containsKey(key));
                    }
                    return rawEntry != null;
                };

                Assert.assertEquals(getRaw.get(key, stream), desired.containsKey(key));
            } finally {
                reader.release();
            }
        }

        System.out.println("Ranges");
        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;

            int[] streamed = new int[1];
            RawEntryStream stream = (rawEntryFormat, entry, offset, length) -> {
                if (entry != null) {
                    System.out.println("Streamed:" + SimpleRawhide.toString(entry));
                    streamed[0]++;
                }
                return true;
            };

            System.out.println("Asked:" + UIO.bytesLong(keys.get(_i)) + " to " + UIO.bytesLong(keys.get(_i + 3)));
            reader = walIndex.acquireReader();
            try {
                NextRawEntry rangeScan = reader.rangeScan(keys.get(_i), keys.get(_i + 3));
                while (rangeScan.next(stream) == NextRawEntry.Next.more);
                Assert.assertEquals(3, streamed[0]);
            } finally {
                reader.release();
            }

        }

        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;
            int[] streamed = new int[1];
            RawEntryStream stream = (rawEntryFormat, entry, offset, length) -> {
                if (entry != null) {
                    streamed[0]++;
                }
                return SimpleRawhide.value(entry) != -1;
            };
            reader = walIndex.acquireReader();
            try {
                NextRawEntry rangeScan = reader.rangeScan(UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1), keys.get(_i + 3));
                while (rangeScan.next(stream) == NextRawEntry.Next.more);
                Assert.assertEquals(2, streamed[0]);
            } finally {
                reader.release();
            }

        }
    }
}
