package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.BolBuffer;
import com.jivesoftware.os.lab.LABConcurrentSkipListMap;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.LabHeapPressure;
import com.jivesoftware.os.lab.StripingBolBufferLocks;
import com.jivesoftware.os.lab.TestUtils;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.NoOpFormatTransformerProvider;
import com.jivesoftware.os.lab.api.RawEntryFormat;
import com.jivesoftware.os.lab.guts.allocators.LABAppendOnlyAllocator;
import com.jivesoftware.os.lab.guts.allocators.LABConcurrentSkipListMemory;
import com.jivesoftware.os.lab.guts.allocators.LABIndexableMemory;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.guts.api.Scanner;
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

        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(simpleRawEntry.getKeyComparator());

        int count = 100;
        int step = 10;

        IndexRangeId indexRangeId = new IndexRangeId(1, 1, 0);

        LABAppendableIndex write = new LABAppendableIndex(indexRangeId, new IndexFile(indexFiler, "rw"),
            64, 10, simpleRawEntry, FormatTransformer.NO_OP, FormatTransformer.NO_OP, new RawEntryFormat(0, 0));

        BolBuffer keyBuffer = new BolBuffer();
        TestUtils.append(new Random(), write, 0, step, count, desired, keyBuffer);
        write.closeAppendable(false);

        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        ReadOnlyIndex leapsAndBoundsIndex = new ReadOnlyIndex(destroy,
            indexRangeId,
            new IndexFile(indexFiler, "r"),
            NoOpFormatTransformerProvider.NO_OP, simpleRawEntry,
            leapsCache);

        assertions(leapsAndBoundsIndex, count, step, desired);
    }

    @Test(enabled = false)
    public void testMemory() throws Exception {

        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(simpleRawEntry.getKeyComparator());

        int count = 10;
        int step = 10;

        ExecutorService destroy = Executors.newSingleThreadExecutor();
        LabHeapPressure labHeapPressure = new LabHeapPressure(LABEnvironment.buildLABHeapSchedulerThreadPool(1), "default", -1, -1,
            new AtomicLong());
        SimpleRawhide rawhide = new SimpleRawhide();
        LABMemoryIndex walIndex = new LABMemoryIndex(destroy,
            labHeapPressure,
            rawhide,
            new LABConcurrentSkipListMap(
                new LABConcurrentSkipListMemory(rawhide,
                    new LABIndexableMemory("memory",
                        new LABAppendOnlyAllocator(30)
                    )
                ),
                new StripingBolBufferLocks(1024)
            ));
        BolBuffer keyBuffer = new BolBuffer();
        TestUtils.append(new Random(), walIndex, 0, step, count, desired, keyBuffer);
        assertions(walIndex, count, step, desired);
    }

    @Test(enabled = false)
    public void testMemoryToDisk() throws Exception {

        ExecutorService destroy = Executors.newSingleThreadExecutor();
        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(simpleRawEntry.getKeyComparator());

        int count = 10;
        int step = 10;
        LabHeapPressure labHeapPressure = new LabHeapPressure(LABEnvironment.buildLABHeapSchedulerThreadPool(1), "default", -1, -1, new AtomicLong());

        SimpleRawhide rawhide = new SimpleRawhide();
        LABMemoryIndex memoryIndex = new LABMemoryIndex(destroy,
            labHeapPressure, rawhide,
            new LABConcurrentSkipListMap(
                new LABConcurrentSkipListMemory(rawhide,
                    new LABIndexableMemory("memory",
                        new LABAppendOnlyAllocator(30)
                    )
                ),
                new StripingBolBufferLocks(1024)
            ));

        BolBuffer keyBuffer = new BolBuffer();
        TestUtils.append(new Random(), memoryIndex, 0, step, count, desired, keyBuffer);
        assertions(memoryIndex, count, step, desired);

        File indexFiler = File.createTempFile("c-index", ".tmp");
        IndexRangeId indexRangeId = new IndexRangeId(1, 1, 0);
        LABAppendableIndex disIndex = new LABAppendableIndex(indexRangeId, new IndexFile(indexFiler, "rw"),
            64, 10, simpleRawEntry, FormatTransformer.NO_OP, FormatTransformer.NO_OP, new RawEntryFormat(0, 0));
        disIndex.append((stream) -> {
            ReadIndex reader = memoryIndex.acquireReader();
            try {
                Scanner rowScan = reader.rowScan();
                RawEntryStream rawStream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                    byte[] bytes = IndexUtil.toByteArray(rawEntry);
                    return stream.stream(readKeyFormatTransformer, readValueFormatTransformer, new BolBuffer(bytes));
                };
                while (rowScan.next(rawStream) == Scanner.Next.more) {
                }
            } finally {
                reader.release();
                reader = null;
            }
            return true;
        }, keyBuffer);
        disIndex.closeAppendable(false);

        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        assertions(new ReadOnlyIndex(destroy, indexRangeId, new IndexFile(indexFiler, "r"), NoOpFormatTransformerProvider.NO_OP, simpleRawEntry,
            leapsCache), count, step, desired);

    }


     private void assertions(LABMemoryIndex memoryIndex, int count, int step, ConcurrentSkipListMap<byte[], byte[]> desired) throws
        Exception {
        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];
        ReadIndex reader = memoryIndex.acquireReader();
        try {
            Scanner rowScan = reader.rowScan();
            RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                System.out.println("rowScan:" + SimpleRawhide.key(rawEntry));
                Assert.assertEquals(UIO.bytesLong(keys.get(index[0])), SimpleRawhide.key(rawEntry));
                index[0]++;
                return true;
            };
            while (rowScan.next(stream) == Scanner.Next.more) {
            }
        } finally {
            reader.release();
            reader = null;
        }
        System.out.println("Point Get");
        for (int i = 0; i < count * step; i++) {
            long k = i;
            reader = memoryIndex.acquireReader();
            try {
                GetRaw getRaw = reader.get();
                byte[] key = UIO.longBytes(k, new byte[8], 0);
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {

                    System.out.println("Got: " + SimpleRawhide.toString(rawEntry));
                    if (rawEntry != null) {
                        byte[] rawKey = UIO.longBytes(SimpleRawhide.key(rawEntry), new byte[8], 0);
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
            RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, entry) -> {
                if (entry != null) {
                    System.out.println("Streamed:" + SimpleRawhide.toString(entry));
                    streamed[0]++;
                }
                return true;
            };

            System.out.println("Asked:" + UIO.bytesLong(keys.get(_i)) + " to " + UIO.bytesLong(keys.get(_i + 3)));
            reader = memoryIndex.acquireReader();
            try {
                Scanner rangeScan = reader.rangeScan(keys.get(_i), keys.get(_i + 3));
                while (rangeScan.next(stream) == Scanner.Next.more) {
                }
                Assert.assertEquals(3, streamed[0]);
            } finally {
                reader.release();
            }

        }

        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;
            int[] streamed = new int[1];
            RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, entry) -> {
                if (entry != null) {
                    streamed[0]++;
                }
                return SimpleRawhide.value(entry) != -1;
            };
            reader = memoryIndex.acquireReader();
            try {
                Scanner rangeScan = reader.rangeScan(UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1, new byte[8], 0), keys.get(_i + 3));
                while (rangeScan.next(stream) == Scanner.Next.more) {
                }
                Assert.assertEquals(2, streamed[0]);
            } finally {
                reader.release();
            }

        }
    }



    private void assertions(ReadOnlyIndex walIndex, int count, int step, ConcurrentSkipListMap<byte[], byte[]> desired) throws
        Exception {
        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];
        ReadIndex reader = walIndex.acquireReader();
        try {
            Scanner rowScan = reader.rowScan();
            RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                System.out.println("rowScan:" + SimpleRawhide.key(rawEntry));
                Assert.assertEquals(UIO.bytesLong(keys.get(index[0])), SimpleRawhide.key(rawEntry));
                index[0]++;
                return true;
            };
            while (rowScan.next(stream) == Scanner.Next.more) {
            }
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
                byte[] key = UIO.longBytes(k, new byte[8], 0);
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {

                    System.out.println("Got: " + SimpleRawhide.toString(rawEntry));
                    if (rawEntry != null) {
                        byte[] rawKey = UIO.longBytes(SimpleRawhide.key(rawEntry), new byte[8], 0);
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
            RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, entry) -> {
                if (entry != null) {
                    System.out.println("Streamed:" + SimpleRawhide.toString(entry));
                    streamed[0]++;
                }
                return true;
            };

            System.out.println("Asked:" + UIO.bytesLong(keys.get(_i)) + " to " + UIO.bytesLong(keys.get(_i + 3)));
            reader = walIndex.acquireReader();
            try {
                Scanner rangeScan = reader.rangeScan(keys.get(_i), keys.get(_i + 3));
                while (rangeScan.next(stream) == Scanner.Next.more) {
                }
                Assert.assertEquals(3, streamed[0]);
            } finally {
                reader.release();
            }

        }

        for (int i = 0; i < keys.size() - 3; i++) {
            int _i = i;
            int[] streamed = new int[1];
            RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, entry) -> {
                if (entry != null) {
                    streamed[0]++;
                }
                return SimpleRawhide.value(entry) != -1;
            };
            reader = walIndex.acquireReader();
            try {
                Scanner rangeScan = reader.rangeScan(UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1, new byte[8], 0), keys.get(_i + 3));
                while (rangeScan.next(stream) == Scanner.Next.more) {
                }
                Assert.assertEquals(2, streamed[0]);
            } finally {
                reader.release();
            }

        }
    }
}
