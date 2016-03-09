package com.jivesoftware.os.lab.guts;

import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.NextRawEntry;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.File;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.jivesoftware.os.lab.guts.SimpleRawEntry.rawEntry;

/**
 *
 * @author jonathan.colt
 */
public class CompactableIndexsNGTest {

    private static OrderIdProvider timeProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));

    @Test(enabled = true)
    public void testPointGets() throws Exception {

        ExecutorService destroy = Executors.newSingleThreadExecutor();
        CompactableIndexes indexs = new CompactableIndexes();
        AtomicLong id = new AtomicLong();
//        int[] counts = new int[1000];
//        Random rand = new Random(1234);
//        for (int i = 0; i < counts.length; i++) {
//            counts[i] = 1 + rand.nextInt(5);
//
//        }
        int[] counts = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30};
        for (int wi = 0; wi < counts.length; wi++) {
            int ci = wi;
            File file = File.createTempFile("a-index-" + wi, ".tmp");
            IndexFile indexFile = new IndexFile(file, "rw", false);
            IndexRangeId indexRangeId = new IndexRangeId(wi, wi, 0);

            int entriesBetweenLeaps = 2;
            int maxLeaps = IndexUtil.calculateIdealMaxLeaps(counts[ci], entriesBetweenLeaps);
            LABAppenableIndex write = new LABAppenableIndex(indexRangeId, indexFile, maxLeaps, entriesBetweenLeaps);

            write.append((stream) -> {
                for (int i = 0; i < counts[ci]; i++) {
                    long time = timeProvider.nextId();
                    byte[] rawEntry = rawEntry(id.incrementAndGet(), time);
                    if (!stream.stream(rawEntry, 0, rawEntry.length)) {
                        break;
                    }
                }
                return true;
            });

            write.closeAppendable(true);

            indexFile = new IndexFile(file, "r", false);
            indexs.append(new LeapsAndBoundsIndex(destroy, indexRangeId, indexFile, 8));
        }

        for (int i = 1; i <= id.get(); i++) {
            long g = i;
            byte[] k = UIO.longBytes(i);
            boolean[] passed = {false};
            System.out.println("Get:" + i);
            indexs.tx(new ReaderTx() {
                @Override
                public boolean tx(ReadIndex[] readIndexs) throws Exception {

//                    GetRaw getRaw = IndexUtil.get(readIndexs);
//                    getRaw.get(k, new RawEntryStream() {
//                        @Override
//                        public boolean stream(byte[] rawEntry, int offset, int length) throws Exception {
//                            System.out.println("\t\tGot:" + UIO.bytesLong(rawEntry, 4));
//                            if (UIO.bytesLong(rawEntry, 4) != g) {
//                                passed[0] = true;
//                            }
//                            return true;
//                        }
//                    });
//                    GetRaw[] pointGets = new GetRaw[readIndexs.length];
//                    for (int i = 0; i < pointGets.length; i++) {
//                        pointGets[i] = readIndexs[i].get();
//                    }
//
//                    for (GetRaw raw : pointGets) {
//                        System.out.println("\tIndex:" + raw);
//                        raw.get(k, new RawEntryStream() {
//                            @Override
//                            public boolean stream(byte[] rawEntry, int offset, int length) throws Exception {
//                                System.out.println("\t\tGot:" + UIO.bytesLong(rawEntry, 4));
//                                if (UIO.bytesLong(rawEntry, 4) == g) {
//                                    passed[0] = true;
//                                }
//                                return true;
//                            }
//                        });
//                    }
                    for (ReadIndex raw : readIndexs) {
                        System.out.println("\tIndex:" + raw);
                        raw.get().get(k, new RawEntryStream() {
                            @Override
                            public boolean stream(byte[] rawEntry, int offset, int length) throws Exception {
                                System.out.println("\t\tGot:" + UIO.bytesLong(rawEntry, 4));
                                if (UIO.bytesLong(rawEntry, 4) == g) {
                                    passed[0] = true;
                                }
                                return true;
                            }
                        });
                    }
                    return true;
                }
            });
            if (!passed[0]) {
                Assert.fail();
            }
        }

    }

    @Test(enabled = false)
    public void testConcurrentMerges() throws Exception {

        ExecutorService destroy = Executors.newSingleThreadExecutor();
        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        int count = 3;
        int step = 100;
        int indexes = 40;
        boolean fsync = true;
        int minimumRun = 4;

        CompactableIndexes indexs = new CompactableIndexes();
        long time = System.currentTimeMillis();
        System.out.println("Seed:" + time);
        Random rand = new Random(1446914103456L);

        Executors.newSingleThreadExecutor().submit(() -> {
            for (int wi = 0; wi < indexes; wi++) {

                File file = File.createTempFile("a-index-" + wi, ".tmp");
                IndexFile indexFile = new IndexFile(file, "rw", false);
                IndexRangeId indexRangeId = new IndexRangeId(wi, wi, 0);

                LABAppenableIndex write = new LABAppenableIndex(indexRangeId, indexFile, 64, 2);
                IndexTestUtils.append(rand, write, 0, step, count, desired);
                write.closeAppendable(fsync);

                indexFile = new IndexFile(file, "r", false);
                indexs.append(new LeapsAndBoundsIndex(destroy, indexRangeId, indexFile, 8));

            }
            Thread.sleep(10);
            return null;
        });

        assertions(indexs, count, step, desired);

        File indexFiler = File.createTempFile("a-index-merged", ".tmp");
        Callable<Void> compactor = indexs.compactor(-1, -1, -1, null, minimumRun, fsync,
            (minimumRun1, fsync1, callback) -> callback.build(minimumRun1,
                fsync1,
                (id, worstCaseCount) -> {
                    int updatesBetweenLeaps = 2;
                    int maxLeaps = IndexUtil.calculateIdealMaxLeaps(worstCaseCount, updatesBetweenLeaps);
                    return new LABAppenableIndex(id, new IndexFile(indexFiler, "rw", false), maxLeaps, updatesBetweenLeaps);
                },
                (ids) -> {
                    return new LeapsAndBoundsIndex(destroy, ids.get(0), new IndexFile(indexFiler, "r", false), 8);
                }));

        if (compactor != null) {
            compactor.call();
        }

        assertions(indexs, count, step, desired);
    }

    @Test(enabled = true)
    public void testTx() throws Exception {

        ExecutorService destroy = Executors.newSingleThreadExecutor();
        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(UnsignedBytes.lexicographicalComparator());

        int count = 3;
        int step = 100;
        int indexes = 4;
        boolean fsync = true;
        int minimumRun = 4;

        CompactableIndexes indexs = new CompactableIndexes();
        long time = System.currentTimeMillis();
        System.out.println("Seed:" + time);
        Random rand = new Random(1446914103456L);
        for (int wi = 0; wi < indexes; wi++) {

            File indexFiler = File.createTempFile("MergableIndexsNGTest" + File.separator + "MergableIndexsNGTest-testTx" + File.separator + "a-index-" + wi,
                ".tmp");
            IndexFile indexFile = new IndexFile(indexFiler, "rw", false);
            IndexRangeId indexRangeId = new IndexRangeId(wi, wi, 0);

            LABAppenableIndex write = new LABAppenableIndex(indexRangeId, indexFile, 64, 2);
            IndexTestUtils.append(rand, write, 0, step, count, desired);
            write.closeAppendable(fsync);

            indexFile = new IndexFile(indexFiler, "r", false);
            indexs.append(new LeapsAndBoundsIndex(destroy, indexRangeId, indexFile, 8));
        }

        indexs.tx((ReadIndex[] readIndexs) -> {
            for (ReadIndex readIndex : readIndexs) {
                System.out.println("---------------------");
                NextRawEntry rowScan = readIndex.rowScan();
                RawEntryStream stream = (rawEntry, offset, length) -> {
                    System.out.println(" Found:" + SimpleRawEntry.toString(rawEntry));
                    return true;
                };
                while (rowScan.next(stream) == NextRawEntry.Next.more);
            }
            return true;
        });

        assertions(indexs, count, step, desired);

        File indexFiler = File.createTempFile("a-index-merged", ".tmp");

        Callable<Void> compactor = indexs.compactor(-1, -1, -1, null, minimumRun, fsync,
            (minimumRun1, fsync1, callback) -> callback.build(minimumRun1,
                fsync1, (id, worstCaseCount) -> {
                    int updatesBetweenLeaps = 2;
                    int maxLeaps = IndexUtil.calculateIdealMaxLeaps(worstCaseCount, updatesBetweenLeaps);
                    return new LABAppenableIndex(id, new IndexFile(indexFiler, "rw", false), maxLeaps, updatesBetweenLeaps);
                }, (ids) -> {
                    return new LeapsAndBoundsIndex(destroy, ids.get(0), new IndexFile(indexFiler, "r", false), 8);
                }));

        if (compactor != null) {
            compactor.call();
        } else {
            Assert.fail();
        }

        indexs.tx((ReadIndex[] readIndexs) -> {
            for (ReadIndex readIndex : readIndexs) {
                System.out.println("---------------------");
                NextRawEntry rowScan = readIndex.rowScan();
                RawEntryStream stream = (rawEntry, offset, length) -> {
                    System.out.println(" Found:" + SimpleRawEntry.toString(rawEntry));
                    return true;
                };
                while (rowScan.next(stream) == NextRawEntry.Next.more);
            }
            return true;
        });

        indexs = new CompactableIndexes();
        IndexRangeId indexRangeId = new IndexRangeId(0, 0, 0);
        IndexFile indexFile = new IndexFile(indexFiler, "r", false);
        indexs.append(new LeapsAndBoundsIndex(destroy, indexRangeId, indexFile, 8));

        assertions(indexs, count, step, desired);
    }

    private void assertions(CompactableIndexes indexs,
        int count, int step,
        ConcurrentSkipListMap<byte[], byte[]> desired) throws
        Exception {

        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];
        indexs.tx(acquired -> {
            NextRawEntry rowScan = IndexUtil.rowScan(acquired);
            AtomicBoolean failed = new AtomicBoolean();
            RawEntryStream stream = (rawEntry, offset, length) -> {
                System.out.println("Expected:key:" + UIO.bytesLong(keys.get(index[0])) + " Found:" + SimpleRawEntry.toString(rawEntry));
                //Assert.assertEquals(UIO.bytesLong(keys.get(index[0])), SimpleRawEntry.key(rawEntry));
                if (UIO.bytesLong(keys.get(index[0])) != SimpleRawEntry.key(rawEntry)) {
                    failed.set(true);
                }
                index[0]++;
                return true;
            };
            while (rowScan.next(stream) == NextRawEntry.Next.more);
            Assert.assertFalse(failed.get());

            Assert.assertEquals(index[0], keys.size());
            System.out.println("rowScan PASSED");
            return true;
        });

        indexs.tx(acquired -> {
            for (int i = 0; i < count * step; i++) {
                long k = i;
                GetRaw getRaw = IndexUtil.get(acquired);
                byte[] key = UIO.longBytes(k);
                RawEntryStream stream = (rawEntry, offset, length) -> {
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
                getRaw.get(key, stream);
            }
            System.out.println("gets PASSED");
            return true;
        });

        indexs.tx(acquired -> {
            for (int i = 0; i < keys.size() - 3; i++) {
                int _i = i;

                int[] streamed = new int[1];
                RawEntryStream stream = (rawEntry, offset, length) -> {
                    if (SimpleRawEntry.value(rawEntry) > -1) {
                        System.out.println("Streamed:" + SimpleRawEntry.toString(rawEntry));
                        streamed[0]++;
                    }
                    return true;
                };

                System.out.println("Asked index:" + _i + " key:" + UIO.bytesLong(keys.get(_i)) + " to:" + UIO.bytesLong(keys.get(_i + 3)));
                NextRawEntry rangeScan = IndexUtil.rangeScan(acquired, keys.get(_i), keys.get(_i + 3));
                while (rangeScan.next(stream) == NextRawEntry.Next.more);
                Assert.assertEquals(3, streamed[0]);

            }
            System.out.println("rangeScan PASSED");
            return true;
        });

        indexs.tx(acquired -> {
            for (int i = 0; i < keys.size() - 3; i++) {
                int _i = i;
                int[] streamed = new int[1];
                RawEntryStream stream = (rawEntry, offset, length) -> {
                    if (SimpleRawEntry.value(rawEntry) > -1) {
                        streamed[0]++;
                    }
                    return true;
                };
                NextRawEntry rangeScan = IndexUtil.rangeScan(acquired, UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1), keys.get(_i + 3));
                while (rangeScan.next(stream) == NextRawEntry.Next.more);
                Assert.assertEquals(2, streamed[0]);

            }
            System.out.println("rangeScan2 PASSED");
            return true;
        });
    }

}
