package com.jivesoftware.os.lab.guts;

import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.lab.guts.api.CommitIndex;
import com.jivesoftware.os.lab.guts.api.IndexFactory;
import com.jivesoftware.os.lab.guts.api.NextRawEntry;
import com.jivesoftware.os.lab.guts.api.RawConcurrentReadableIndex;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * @author jonathan.colt
 */
public class MergeableIndexes {

    static private class IndexesLock {
    }

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    // newest to oldest
    private final IndexesLock indexesLock = new IndexesLock();
    private volatile boolean[] merging = new boolean[0];
    private volatile RawConcurrentReadableIndex[] indexes = new RawConcurrentReadableIndex[0];
    private volatile long version;
    private volatile boolean disposed = false;

    public boolean append(RawConcurrentReadableIndex index) {
        synchronized (indexesLock) {
            if (disposed) {
                return false;
            }

            int length = indexes.length + 1;
            boolean[] prependToMerging = new boolean[length];
            prependToMerging[0] = false;
            System.arraycopy(merging, 0, prependToMerging, 1, merging.length);

            RawConcurrentReadableIndex[] prependToIndexes = new RawConcurrentReadableIndex[length];
            prependToIndexes[0] = index;
            System.arraycopy(indexes, 0, prependToIndexes, 1, indexes.length);

            merging = prependToMerging;
            indexes = prependToIndexes;
            version++;
        }
        return true;
    }

    public boolean splittable(long splittableIfLargerThanBytes) throws Exception {
        RawConcurrentReadableIndex[] splittable;
        synchronized (indexesLock) {
            if (disposed || indexes.length == 0) {
                return false;
            }
            splittable = indexes;
        }
        byte[] minKey = null;
        byte[] maxKey = null;
        long worstCaseSizeInBytes = 0;
        for (int i = 0; i < splittable.length; i++) {
            worstCaseSizeInBytes += splittable[i].sizeInBytes();
            if (minKey == null) {
                minKey = splittable[i].minKey();
            } else {
                minKey = UnsignedBytes.lexicographicalComparator().compare(minKey, splittable[i].minKey()) < 0 ? minKey : splittable[i].minKey();
            }

            if (maxKey == null) {
                maxKey = splittable[i].maxKey();
            } else {
                maxKey = UnsignedBytes.lexicographicalComparator().compare(maxKey, splittable[i].maxKey()) < 0 ? maxKey : splittable[i].maxKey();
            }
        }

        if (Arrays.equals(minKey, maxKey)) {
            return false;
        }

        return (worstCaseSizeInBytes > splittableIfLargerThanBytes);
    }

    public void split(IndexFactory leftHalfIndexFactory,
        IndexFactory rightHalfIndexFactory,
        CommitIndex commitIndex,
        boolean fsync) throws Exception {

        IN_THE_BEGINNING:
        while (true) {
            try {
                long startVerion;
                RawConcurrentReadableIndex[] mergeAll;
                int splitLength;
                synchronized (indexesLock) {
                    startVerion = version;
                    for (boolean b : merging) {
                        if (b) {
                            indexesLock.wait();
                            continue IN_THE_BEGINNING;
                        }
                    }
                    Arrays.fill(merging, true);
                    mergeAll = indexes;
                    splitLength = merging.length;
                }

                long worstCaseCount = 0;
                NextRawEntry[] feeders = new NextRawEntry[mergeAll.length];
                IndexRangeId join = null;
                byte[] minKey = null;
                byte[] maxKey = null;
                for (int i = 0; i < mergeAll.length; i++) {
                    ReadIndex readIndex = mergeAll[i].reader();
                    worstCaseCount += readIndex.count();
                    feeders[i] = readIndex.rowScan();
                    IndexRangeId id = mergeAll[i].id();
                    if (join == null) {
                        join = new IndexRangeId(id.start, id.end, id.generation + 1);
                    } else {
                        join = join.join(id, Math.max(join.generation, id.generation + 1));
                    }

                    if (minKey == null) {
                        minKey = mergeAll[i].minKey();
                    } else {
                        minKey = UnsignedBytes.lexicographicalComparator().compare(minKey, mergeAll[i].minKey()) < 0 ? minKey : mergeAll[i].minKey();
                    }

                    if (maxKey == null) {
                        maxKey = mergeAll[i].maxKey();
                    } else {
                        maxKey = UnsignedBytes.lexicographicalComparator().compare(maxKey, mergeAll[i].maxKey()) < 0 ? maxKey : mergeAll[i].maxKey();
                    }
                }

                if (Arrays.equals(minKey, maxKey)) {
                    // TODO how not to get here over an over again when a key is larger that split size in byte Cannot split a single key
                    return;
                } else {
                    byte[] middle = Lists.newArrayList(UIO.iterateOnSplits(minKey, maxKey, true, 1)).get(1);
                    LABAppenableIndex leftAppenableIndex = leftHalfIndexFactory.createIndex(join, worstCaseCount - 1);
                    LABAppenableIndex rightAppenableIndex = rightHalfIndexFactory.createIndex(join, worstCaseCount - 1);
                    InterleaveStream feedInterleaver = new InterleaveStream(feeders);

                    leftAppenableIndex.append((leftStream) -> {
                        return rightAppenableIndex.append((rightStream) -> {
                            boolean more = true;
                            while (more) {
                                more = feedInterleaver.next((rawEntry, offset, length) -> {
                                    if (UnsignedBytes.lexicographicalComparator().compare(rawEntry, middle) < 0) {
                                        if (!leftStream.stream(rawEntry, offset, length)) {
                                            return false;
                                        }
                                    } else if (!rightStream.stream(rawEntry, offset, length)) {
                                        return false;
                                    }
                                    return true;
                                });
                            }
                            return true;
                        });
                    });

                    leftAppenableIndex.closeAppendable(fsync);
                    rightAppenableIndex.closeAppendable(fsync);

                    List<IndexRangeId> commitRanges = new ArrayList<>();
                    commitRanges.add(join);

                    CATCHUP_YOU_BABY_TOMATO:
                    while (true) {
                        RawConcurrentReadableIndex[] catchupMergeSet;
                        synchronized (indexesLock) {
                            if (startVerion == version) {
                                commitIndex.commit(commitRanges);
                                for (RawConcurrentReadableIndex destroy : indexes) {
                                    destroy.destroy();
                                }
                                version++;
                                indexes = new RawConcurrentReadableIndex[0]; // TODO go handle null so that thread wait rety higher up
                                merging = new boolean[0];
                                disposed = true;
                                return;
                            } else {
                                int catchupLength = merging.length - splitLength;
                                for (int i = splitLength; i < merging.length; i++) {
                                    if (merging[i]) {
                                        indexesLock.wait();
                                        continue CATCHUP_YOU_BABY_TOMATO;
                                    }
                                }
                                startVerion = version;
                                catchupMergeSet = new RawConcurrentReadableIndex[catchupLength];
                                Arrays.fill(merging, splitLength, catchupLength, true);
                                System.arraycopy(indexes, splitLength, catchupMergeSet, 0, catchupLength);
                                splitLength = merging.length;
                            }
                        }

                        for (RawConcurrentReadableIndex catchup : catchupMergeSet) {
                            IndexRangeId id = catchup.id();
                            LABAppenableIndex catupLeftAppenableIndex = leftHalfIndexFactory.createIndex(id, catchup.count());
                            LABAppenableIndex catchupRightAppenableIndex = rightHalfIndexFactory.createIndex(id, catchup.count());
                            InterleaveStream catchupFeedInterleaver = new InterleaveStream(new NextRawEntry[]{catchup.reader().rowScan()});

                            catupLeftAppenableIndex.append((leftStream) -> {
                                return catchupRightAppenableIndex.append((rightStream) -> {
                                    boolean more = true;
                                    while (more) {
                                        more = catchupFeedInterleaver.next((rawEntry, offset, length) -> {
                                            if (UnsignedBytes.lexicographicalComparator().compare(rawEntry, middle) < 0) {
                                                if (!leftStream.stream(rawEntry, offset, length)) {
                                                    return false;
                                                }
                                            } else if (!rightStream.stream(rawEntry, offset, length)) {
                                                return false;
                                            }
                                            return true;
                                        });
                                    }
                                    return true;
                                });
                            });

                            catupLeftAppenableIndex.closeAppendable(fsync);
                            catchupRightAppenableIndex.closeAppendable(fsync);

                            commitRanges.add(0, id);
                        }

                    }

                }

            } catch (Exception x) {
                synchronized (indexesLock) {
                    Arrays.fill(merging, true);
                }
                throw x;
            }
        }

    }

    public int hasMergeDebt(int minimumRun) throws IOException {
        boolean[] mergingCopy;
        RawConcurrentReadableIndex[] indexesCopy;
        synchronized (indexesLock) {
            if (indexes == null) {
                return 0;
            }
            mergingCopy = Arrays.copyOf(merging, merging.length);
            indexesCopy = indexes;
        }
        return indexesCopy.length;
        /*
        long[] counts = new long[indexesCopy.length];
        long[] generations = new long[indexesCopy.length];
        byte[][] minKeys = new byte[indexesCopy.length][];
        byte[][] maxKeys = new byte[indexesCopy.length][];

        for (int i = 0; i < counts.length; i++) {
            counts[i] = indexesCopy[i].count();
            generations[i] = indexesCopy[i].id().generation;
            minKeys[i] = indexesCopy[i].minKey();
            maxKeys[i] = indexesCopy[i].maxKey();
        }

        int debt = 0;
        while (true) {
            MergeRange mergeRange = TieredCompaction.getMergeRange(minimumRun, mergingCopy, counts, generations, minKeys, maxKeys);
            if (mergeRange == null) {
                for (boolean m : mergingCopy) {
                    if (m) {
                        debt++;
                    }
                }
                return debt;
            } else {
                debt++;
                Arrays.fill(mergingCopy, mergeRange.offset, mergeRange.offset + mergeRange.length, true);
            }
        }*/
    }

    public Merger buildMerger(int minimumRun, IndexFactory indexFactory, CommitIndex commitIndex, boolean fsync) throws Exception {
        boolean[] mergingCopy;
        RawConcurrentReadableIndex[] indexesCopy;
        RawConcurrentReadableIndex[] mergeSet;
        MergeRange mergeRange;
        long[] counts;
        long[] sizes;
        long[] generations;
        synchronized (indexesLock) { // prevent others from trying to merge the same things
            if (indexes == null || indexes.length <= 1) {
                return null;
            }

            mergingCopy = merging;
            indexesCopy = indexes;

            counts = new long[indexesCopy.length];
            sizes = new long[indexesCopy.length];
            generations = new long[indexesCopy.length];
            byte[][] minKeys = new byte[indexesCopy.length][];
            byte[][] maxKeys = new byte[indexesCopy.length][];

            for (int i = 0; i < counts.length; i++) {
                counts[i] = indexesCopy[i].count();
                generations[i] = indexesCopy[i].id().generation;
                minKeys[i] = indexesCopy[i].minKey();
                maxKeys[i] = indexesCopy[i].maxKey();
                sizes[i] = indexesCopy[i].sizeInBytes();
            }

            mergeRange = TieredCompaction.getMergeRange(minimumRun, mergingCopy, counts, sizes, generations, minKeys, maxKeys);
            if (mergeRange == null) {
                return null;
            }

            mergeSet = new RawConcurrentReadableIndex[mergeRange.length];
            System.arraycopy(indexesCopy, mergeRange.offset, mergeSet, 0, mergeRange.length);

            boolean[] updateMerging = new boolean[merging.length];
            System.arraycopy(merging, 0, updateMerging, 0, merging.length);
            Arrays.fill(updateMerging, mergeRange.offset, mergeRange.offset + mergeRange.length, true);
            merging = updateMerging;
        }

        IndexRangeId join = null;
        for (RawConcurrentReadableIndex m : mergeSet) {
            IndexRangeId id = m.id();
            if (join == null) {
                join = new IndexRangeId(id.start, id.end, mergeRange.generation + 1);
            } else {
                join = join.join(id, Math.max(join.generation, id.generation));
            }
        }

        return new Merger(counts, generations, mergeSet, join, indexFactory, commitIndex, fsync, mergeRange);
    }

    public class Merger implements Callable<LeapsAndBoundsIndex> {

        private final long[] counts;
        private final long[] generations;
        private final RawConcurrentReadableIndex[] mergeSet;
        private final IndexRangeId mergeRangeId;
        private final IndexFactory indexFactory;
        private final CommitIndex commitIndex;
        private final boolean fsync;
        private final MergeRange mergeRange;

        private Merger(
            long[] counts,
            long[] generations,
            RawConcurrentReadableIndex[] mergeSet,
            IndexRangeId mergeRangeId,
            IndexFactory indexFactory,
            CommitIndex commitIndex,
            boolean fsync,
            MergeRange mergeRange) {

            this.mergeRange = mergeRange;
            this.counts = counts;
            this.generations = generations;
            this.mergeSet = mergeSet;
            this.mergeRangeId = mergeRangeId;
            this.indexFactory = indexFactory;
            this.commitIndex = commitIndex;
            this.fsync = fsync;
        }

        @Override
        public String toString() {
            return "Merger{" + "mergeRangeId=" + mergeRangeId + '}';
        }

        @Override
        public LeapsAndBoundsIndex call() throws Exception {
            LeapsAndBoundsIndex index = null;
            try {

                LOG.info("Merging: counts:{} gens:{}...",
                    TieredCompaction.range(counts, mergeRange.offset, mergeRange.length),
                    Arrays.toString(generations)
                );

                long startMerge = System.currentTimeMillis();

                long worstCaseCount = 0;
                NextRawEntry[] feeders = new NextRawEntry[mergeSet.length];
                for (int i = 0; i < feeders.length; i++) {
                    ReadIndex readIndex = mergeSet[i].reader();
                    worstCaseCount += readIndex.count();
                    feeders[i] = readIndex.rowScan();
                }

                LABAppenableIndex appenableIndex = indexFactory.createIndex(mergeRangeId, worstCaseCount);
                InterleaveStream feedInterleaver = new InterleaveStream(feeders);
                appenableIndex.append((stream) -> {
                    while (feedInterleaver.next(stream)) ;
                    return true;
                });
                appenableIndex.closeAppendable(fsync);

                index = commitIndex.commit(Arrays.asList(mergeRangeId));

                synchronized (indexesLock) {
                    int newLength = (indexes.length - mergeSet.length) + 1;
                    boolean[] updateMerging = new boolean[newLength];
                    RawConcurrentReadableIndex[] updateIndexes = new RawConcurrentReadableIndex[newLength];

                    int ui = 0;
                    int mi = 0;
                    for (int i = 0; i < indexes.length; i++) {
                        if (mi < mergeSet.length && indexes[i] == mergeSet[mi]) {
                            if (mi == 0) {
                                updateMerging[ui] = false;
                                updateIndexes[ui] = index;
                                ui++;
                            }
                            mi++;
                        } else {
                            updateMerging[ui] = merging[i];
                            updateIndexes[ui] = indexes[i];
                            ui++;
                        }
                    }

                    merging = updateMerging;
                    indexes = updateIndexes;
                    version++;
                }

                LOG.info("Merged:  {} millis counts:{} gens:{} {}",
                    (System.currentTimeMillis() - startMerge),
                    TieredCompaction.range(counts, mergeRange.offset, mergeRange.length),
                    Arrays.toString(generations),
                    index.name()
                );

                for (RawConcurrentReadableIndex rawConcurrentReadableIndex : mergeSet) {
                    rawConcurrentReadableIndex.destroy();
                }

            } catch (Exception x) {
                LOG.warn("Failed to merge range:" + mergeRangeId, x);

                synchronized (indexesLock) {
                    boolean[] updateMerging = new boolean[merging.length];
                    int mi = 0;
                    for (int i = 0; i < indexes.length; i++) {
                        if (mi < mergeSet.length && indexes[i] == mergeSet[mi]) {
                            updateMerging[i] = false;
                            mi++;
                        } else {
                            updateMerging[i] = merging[i];
                        }
                    }
                    merging = updateMerging;
                }
            }

            return index;
        }

    }

    public boolean tx(ReaderTx tx) throws Exception {

        long cacheVersion = -1;
        RawConcurrentReadableIndex[] stackIndexes;

        ReadIndex[] readIndexs = null;
        TRY_AGAIN:
        while (true) {
            if (readIndexs == null || cacheVersion < version) {

                START_OVER:
                while (true) {
                    long stackVersion = version;
                    synchronized (indexesLock) {
                        stackIndexes = indexes;
                        cacheVersion = stackVersion;
                    }
                    readIndexs = new ReadIndex[stackIndexes.length];
                    for (int i = 0; i < readIndexs.length; i++) {
                        readIndexs[i] = stackIndexes[i].reader();
                        if (readIndexs[i] == null) {
                            continue START_OVER;
                        }
                    }
                    break;
                }
            }
            for (int i = 0; i < readIndexs.length; i++) {
                ReadIndex readIndex = readIndexs[i];
                if (!readIndex.acquire()) {
                    for (int j = 0; j < i; j++) {
                        readIndexs[j].release();
                    }
                    continue TRY_AGAIN;
                }
            }
            break;
        }
        try {
            return tx.tx(readIndexs);
        } finally {
            for (ReadIndex readIndex : readIndexs) {
                readIndex.release();
            }
        }
    }

    public long count() throws Exception {
        long count = 0;
        for (RawConcurrentReadableIndex g : grab()) {
            count += g.count();
        }
        return count;
    }

    public void close() throws Exception {
        synchronized (indexesLock) {
            for (RawConcurrentReadableIndex index : indexes) {
                index.closeReadable();
            }
        }
    }

    public boolean isEmpty() throws Exception {
        for (RawConcurrentReadableIndex g : grab()) {
            if (!g.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private RawConcurrentReadableIndex[] grab() {
        RawConcurrentReadableIndex[] copy;
        synchronized (indexesLock) {
            copy = indexes;
        }
        return copy;
    }

}
