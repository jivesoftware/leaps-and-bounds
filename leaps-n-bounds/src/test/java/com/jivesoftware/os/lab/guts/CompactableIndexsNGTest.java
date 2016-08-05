package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.lab.LABEnvironment;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.FormatTransformerProvider;
import com.jivesoftware.os.lab.api.RawEntryFormat;
import com.jivesoftware.os.lab.api.Rawhide;
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

import static com.jivesoftware.os.lab.guts.SimpleRawhide.rawEntry;

/**
 *
 * @author jonathan.colt
 */
public class CompactableIndexsNGTest {

    private static OrderIdProvider timeProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));
    private final SimpleRawhide simpleRawEntry = new SimpleRawhide();

    @Test(enabled = true)
    public void testPointGets() throws Exception {

        ExecutorService destroy = Executors.newSingleThreadExecutor();
        CompactableIndexes indexs = new CompactableIndexes(new SimpleRawhide());
        AtomicLong id = new AtomicLong();

        int[] counts = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30};
        for (int wi = 0; wi < counts.length; wi++) {
            int ci = wi;
            File file = File.createTempFile("a-index-" + wi, ".tmp");
            IndexFile indexFile = new IndexFile(file, "rw", false);
            IndexRangeId indexRangeId = new IndexRangeId(wi, wi, 0);

            int entriesBetweenLeaps = 2;
            int maxLeaps = RangeStripedCompactableIndexes.calculateIdealMaxLeaps(counts[ci], entriesBetweenLeaps);
            LABAppendableIndex write = new LABAppendableIndex(indexRangeId, indexFile, maxLeaps, entriesBetweenLeaps, simpleRawEntry, FormatTransformer.NO_OP,
                FormatTransformer.NO_OP,
                RawEntryFormat.MEMORY);

            write.append((stream) -> {
                for (int i = 0; i < counts[ci]; i++) {
                    long time = timeProvider.nextId();
                    byte[] rawEntry = rawEntry(id.incrementAndGet(), time);
                    if (!stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, rawEntry, 0, rawEntry.length)) {
                        break;
                    }
                }
                return true;
            });
            write.closeAppendable(true);

            indexFile = new IndexFile(file, "r", false);
            LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
            indexs.append(new LeapsAndBoundsIndex(destroy, indexRangeId, indexFile, FormatTransformerProvider.NO_OP, simpleRawEntry, leapsCache));
        }

      
        for (int i = 1; i <= id.get(); i++) {
            long g = i;
            byte[] k = UIO.longBytes(i);
            boolean[] passed = {false};
            System.out.println("Get:" + i);
            indexs.tx(-1, null, null, (index, fromKey, toKey, readIndexs, hydrateValues) -> {

                for (ReadIndex raw : readIndexs) {
                    System.out.println("\tIndex:" + raw);
                    raw.get().get(k, (readKeyFormatTransformer, readValueFormatTransformer, rawEntry, offset, length) -> {
                        System.out.println("\t\tGot:" + UIO.bytesLong(rawEntry, 4));
                        if (UIO.bytesLong(rawEntry, 4) == g) {
                            passed[0] = true;
                        }
                        return true;
                    });
                }
                return true;
            }, true);
            if (!passed[0]) {
                Assert.fail();
            }
        }

    }

    @Test(enabled = false)
    public void testConcurrentMerges() throws Exception {

        ExecutorService destroy = Executors.newSingleThreadExecutor();
        Rawhide rawhide = new SimpleRawhide();
        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(rawhide);

        int count = 3;
        int step = 100;
        int indexes = 40;
        boolean fsync = true;
        int minimumRun = 4;

        CompactableIndexes indexs = new CompactableIndexes(rawhide);
        long time = System.currentTimeMillis();
        System.out.println("Seed:" + time);
        Random rand = new Random(1446914103456L);

        Executors.newSingleThreadExecutor().submit(() -> {
            for (int wi = 0; wi < indexes; wi++) {

                File file = File.createTempFile("a-index-" + wi, ".tmp");
                IndexFile indexFile = new IndexFile(file, "rw", false);
                IndexRangeId indexRangeId = new IndexRangeId(wi, wi, 0);

                LABAppendableIndex write = new LABAppendableIndex(indexRangeId, indexFile, 64, 2, simpleRawEntry, FormatTransformer.NO_OP,
                    FormatTransformer.NO_OP, new RawEntryFormat(0, 0));
                IndexTestUtils.append(rand, write, 0, step, count, desired);
                write.closeAppendable(fsync);

                indexFile = new IndexFile(file, "r", false);
                LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
                indexs.append(new LeapsAndBoundsIndex(destroy, indexRangeId, indexFile, FormatTransformerProvider.NO_OP, simpleRawEntry, leapsCache));

            }
            Thread.sleep(10);
            return null;
        });

        assertions(indexs, count, step, desired);

        File indexFiler = File.createTempFile("a-index-merged", ".tmp");
        Callable<Void> compactor = indexs.compactor(-1, -1, -1, null, minimumRun, fsync,
            (minimumRun1, fsync1, callback) -> callback.call(minimumRun,
                fsync1,
                (id, worstCaseCount) -> {
                    int updatesBetweenLeaps = 2;
                    int maxLeaps = RangeStripedCompactableIndexes.calculateIdealMaxLeaps(worstCaseCount, updatesBetweenLeaps);
                    return new LABAppendableIndex(id,
                        new IndexFile(indexFiler, "rw", false),
                        maxLeaps,
                        updatesBetweenLeaps,
                        simpleRawEntry,
                        FormatTransformer.NO_OP,
                        FormatTransformer.NO_OP,
                        new RawEntryFormat(0, 0));
                },
                (ids) -> {
                    LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
                    return new LeapsAndBoundsIndex(destroy, ids.get(0), new IndexFile(indexFiler, "r", false), FormatTransformerProvider.NO_OP, simpleRawEntry,
                        leapsCache);
                }));

        if (compactor != null) {
            compactor.call();
        }

        assertions(indexs, count, step, desired);
    }

    @Test(enabled = true)
    public void testTx() throws Exception {

        ExecutorService destroy = Executors.newSingleThreadExecutor();
        Rawhide rawhide = new SimpleRawhide();
        ConcurrentSkipListMap<byte[], byte[]> desired = new ConcurrentSkipListMap<>(rawhide);

        int count = 3;
        int step = 100;
        int indexes = 4;
        boolean fsync = true;
        int minimumRun = 4;

        CompactableIndexes indexs = new CompactableIndexes(rawhide);
        long time = System.currentTimeMillis();
        System.out.println("Seed:" + time);
        Random rand = new Random(1446914103456L);
        for (int wi = 0; wi < indexes; wi++) {

            File indexFiler = File.createTempFile("MergableIndexsNGTest" + File.separator + "MergableIndexsNGTest-testTx" + File.separator + "a-index-" + wi,
                ".tmp");
            IndexFile indexFile = new IndexFile(indexFiler, "rw", false);
            IndexRangeId indexRangeId = new IndexRangeId(wi, wi, 0);

            LABAppendableIndex write = new LABAppendableIndex(indexRangeId, indexFile, 64, 2, simpleRawEntry, FormatTransformer.NO_OP, FormatTransformer.NO_OP,
                new RawEntryFormat(0, 0));
            IndexTestUtils.append(rand, write, 0, step, count, desired);
            write.closeAppendable(fsync);

            indexFile = new IndexFile(indexFiler, "r", false);
            LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
            indexs.append(new LeapsAndBoundsIndex(destroy, indexRangeId, indexFile, FormatTransformerProvider.NO_OP, simpleRawEntry, leapsCache));
        }

        indexs.tx(-1, null, null, (index1, fromKey, toKey, readIndexs, hydrateValues) -> {
            for (ReadIndex readIndex : readIndexs) {
                System.out.println("---------------------");
                NextRawEntry rowScan = readIndex.rowScan();
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry, offset, length) -> {
                    System.out.println(" Found:" + SimpleRawhide.toString(rawEntry));
                    return true;
                };
                while (rowScan.next(stream) == NextRawEntry.Next.more);
            }
            return true;
        }, true);

        assertions(indexs, count, step, desired);

        File indexFiler = File.createTempFile("a-index-merged", ".tmp");

        Callable<Void> compactor = indexs.compactor(-1, -1, -1, null, minimumRun,fsync,
            (minimumRun1, fsync1, callback) -> callback.call(minimumRun1,
                fsync1, (id, worstCaseCount) -> {
                    int updatesBetweenLeaps = 2;
                    int maxLeaps = RangeStripedCompactableIndexes.calculateIdealMaxLeaps(worstCaseCount, updatesBetweenLeaps);
                    return new LABAppendableIndex(id, new IndexFile(indexFiler, "rw", false), maxLeaps, updatesBetweenLeaps, simpleRawEntry,
                        FormatTransformer.NO_OP,
                        FormatTransformer.NO_OP,
                        new RawEntryFormat(0, 0));
                }, (ids) -> {
                    LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
                    return new LeapsAndBoundsIndex(destroy, ids.get(0), new IndexFile(indexFiler, "r", false), FormatTransformerProvider.NO_OP, simpleRawEntry,
                        leapsCache);
                }));

        if (compactor != null) {
            compactor.call();
        } else {
            Assert.fail();
        }

        indexs.tx(-1, null, null, (index1, fromKey, toKey, readIndexs, hydrateValues) -> {
            for (ReadIndex readIndex : readIndexs) {
                System.out.println("---------------------");
                NextRawEntry rowScan = readIndex.rowScan();
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry, offset, length) -> {
                    System.out.println(" Found:" + SimpleRawhide.toString(rawEntry));
                    return true;
                };
                while (rowScan.next(stream) == NextRawEntry.Next.more);
            }
            return true;
        }, true);

        indexs = new CompactableIndexes(rawhide);
        IndexRangeId indexRangeId = new IndexRangeId(0, 0, 0);
        IndexFile indexFile = new IndexFile(indexFiler, "r", false);
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache = LABEnvironment.buildLeapsCache(100, 8);
        indexs.append(new LeapsAndBoundsIndex(destroy, indexRangeId, indexFile, FormatTransformerProvider.NO_OP, simpleRawEntry, leapsCache));

        assertions(indexs, count, step, desired);
    }

    private void assertions(CompactableIndexes indexs,
        int count, int step,
        ConcurrentSkipListMap<byte[], byte[]> desired) throws
        Exception {

        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];
        SimpleRawhide rawhide = new SimpleRawhide();
        indexs.tx(-1, null, null, (index1, fromKey, toKey, acquired, hydrateValues) -> {
            NextRawEntry rowScan = IndexUtil.rowScan(acquired, rawhide);
            AtomicBoolean failed = new AtomicBoolean();
            RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry, offset, length) -> {
                System.out.println("Expected:key:" + UIO.bytesLong(keys.get(index[0])) + " Found:" + SimpleRawhide.toString(rawEntry));
                //Assert.assertEquals(UIO.bytesLong(keys.get(index[0])), SimpleRawEntry.key(rawEntry));
                if (UIO.bytesLong(keys.get(index[0])) != SimpleRawhide.key(rawEntry)) {
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
        }, true);

        indexs.tx(-1, null, null, (index1, fromKey, toKey, acquired, hydrateValues) -> {
            for (int i = 0; i < count * step; i++) {
                long k = i;
                GetRaw getRaw = IndexUtil.get(acquired);
                byte[] key = UIO.longBytes(k);
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry, offset, length) -> {
                    System.out.println("->" + SimpleRawhide.key(rawEntry) + " " + SimpleRawhide.value(rawEntry) + " " + offset + " " + length);
                    if (rawEntry != null) {
                        System.out.println("Got: " + SimpleRawhide.toString(rawEntry));
                        byte[] rawKey = UIO.longBytes(SimpleRawhide.key(rawEntry));
                        Assert.assertEquals(rawKey, key);
                        byte[] d = desired.get(key);
                        if (d == null) {
                            Assert.fail();
                        } else {
                            Assert.assertEquals(SimpleRawhide.value(rawEntry), SimpleRawhide.value(d));
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
        }, true);

        indexs.tx(-1, null, null, (index1, fromKey, toKey, acquired, hydrateValues) -> {
            for (int i = 0; i < keys.size() - 3; i++) {
                int _i = i;

                int[] streamed = new int[1];
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry, offset, length) -> {
                    if (SimpleRawhide.value(rawEntry) > -1) {
                        System.out.println("Streamed:" + SimpleRawhide.toString(rawEntry));
                        streamed[0]++;
                    }
                    return true;
                };

                System.out.println("Asked index:" + _i + " key:" + UIO.bytesLong(keys.get(_i)) + " to:" + UIO.bytesLong(keys.get(_i + 3)));
                NextRawEntry rangeScan = IndexUtil.rangeScan(acquired, keys.get(_i), keys.get(_i + 3), rawhide);
                while (rangeScan.next(stream) == NextRawEntry.Next.more);
                Assert.assertEquals(3, streamed[0]);

            }
            System.out.println("rangeScan PASSED");
            return true;
        }, true);

        indexs.tx(-1, null, null, (index1, fromKey, toKey, acquired, hydrateValues) -> {
            for (int i = 0; i < keys.size() - 3; i++) {
                int _i = i;
                int[] streamed = new int[1];
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry, offset, length) -> {
                    if (SimpleRawhide.value(rawEntry) > -1) {
                        streamed[0]++;
                    }
                    return true;
                };
                NextRawEntry rangeScan = IndexUtil.rangeScan(acquired, UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1), keys.get(_i + 3), rawhide);
                while (rangeScan.next(stream) == NextRawEntry.Next.more);
                Assert.assertEquals(2, streamed[0]);

            }
            System.out.println("rangeScan2 PASSED");
            return true;
        }, true);
    }

}
