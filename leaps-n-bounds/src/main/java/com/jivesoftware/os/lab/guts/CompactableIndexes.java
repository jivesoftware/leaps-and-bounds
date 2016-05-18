package com.jivesoftware.os.lab.guts;

import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedBytes;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.guts.api.CommitIndex;
import com.jivesoftware.os.lab.guts.api.IndexFactory;
import com.jivesoftware.os.lab.guts.api.MergerBuilder;
import com.jivesoftware.os.lab.guts.api.NextRawEntry;
import com.jivesoftware.os.lab.guts.api.RawConcurrentReadableIndex;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.guts.api.SplitterBuilder;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class CompactableIndexes {

    static private class IndexesLock {
    }

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    // newest to oldest
    private final Rawhide rawhide;
    private final IndexesLock indexesLock = new IndexesLock();
    private volatile boolean[] merging = new boolean[0]; // is volatile for reference changes not value changes.
    private volatile RawConcurrentReadableIndex[] indexes = new RawConcurrentReadableIndex[0];  // is volatile for reference changes not value changes.
    private volatile long version;
    private volatile boolean disposed = false;
    private final AtomicLong compactorCheckVersion = new AtomicLong();
    private final AtomicBoolean compacting = new AtomicBoolean();
    private volatile TimestampAndVersion maxTimestampAndVersion = TimestampAndVersion.NULL;

    public CompactableIndexes(Rawhide rawhide) {
        this.rawhide = rawhide;
    }

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
            refreshMaxTimestamp(prependToIndexes);
            version++;
        }
        return true;
    }

    private void refreshMaxTimestamp(RawConcurrentReadableIndex[] concurrentReadableIndexs) {
        TimestampAndVersion timestampAndVersion = TimestampAndVersion.NULL;
        for (RawConcurrentReadableIndex rawConcurrentReadableIndex : concurrentReadableIndexs) {
            TimestampAndVersion other = rawConcurrentReadableIndex.maxTimestampAndVersion();
            if (rawhide.isNewerThan(other.maxTimestamp,
                other.maxTimestampVersion,
                timestampAndVersion.maxTimestamp,
                timestampAndVersion.maxTimestampVersion)) {
                timestampAndVersion = other;
            }
        }
        maxTimestampAndVersion = timestampAndVersion;
    }

    public TimestampAndVersion maxTimeStampAndVersion() {
        return maxTimestampAndVersion;
    }

    public int debt(int mergableIfDebtLargerThan) {
        if (disposed) {
            return 0;
        }
        int debt = merging.length - mergableIfDebtLargerThan;
        return debt < 0 ? 0 : debt + 1;
    }

    public Callable<Void> compactor(
        long splittableIfKeysLargerThanBytes,
        long splittableIfValuesLargerThanBytes,
        long splittableIfLargerThanBytes,
        SplitterBuilder splitterBuilder,
        int mergableIfDebtLargerThan,
        boolean fsync,
        MergerBuilder mergerBuilder
    ) throws Exception {

        long start = compactorCheckVersion.incrementAndGet();
        if (disposed || !compacting.compareAndSet(false, true)) {
            return null;
        }

        while (true) {
            if (splittable(splittableIfKeysLargerThanBytes, splittableIfValuesLargerThanBytes, splittableIfLargerThanBytes)) {
                Callable<Void> splitter = splitterBuilder.build(fsync, this::buildSplitter);
                if (splitter != null) {
                    return () -> {
                        try {
                            return splitter.call();
                        } finally {
                            compacting.set(false);
                        }
                    };
                }
            } else if (debt(mergableIfDebtLargerThan) > 0) {
                Callable<Void> merger = mergerBuilder.build(mergableIfDebtLargerThan, fsync, this::buildMerger);
                if (merger != null) {
                    return () -> {
                        try {
                            return merger.call();
                        } finally {
                            compacting.set(false);
                        }
                    };
                }
            }
            if (start < compactorCheckVersion.get()) {
                start = compactorCheckVersion.get();
            } else {
                compacting.set(false);
                return null;
            }
        }

    }

    private boolean splittable(
        long splittableIfKeysLargerThanBytes,
        long splittableIfValuesLargerThanBytes,
        long splittableIfLargerThanBytes) throws Exception {

        RawConcurrentReadableIndex[] splittable;
        synchronized (indexesLock) {
            if (disposed || indexes.length == 0) {
                return false;
            }
            splittable = indexes;
        }
        byte[] minKey = null;
        byte[] maxKey = null;
        long worstCaseKeysSizeInBytes = 0;
        long worstCaseValuesSizeInBytes = 0;
        long worstCaseSizeInBytes = 0;
        for (int i = 0; i < splittable.length; i++) {
            worstCaseKeysSizeInBytes += splittable[i].keysSizeInBytes();
            worstCaseValuesSizeInBytes += splittable[i].valuesSizeInBytes();
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

        if (splittableIfLargerThanBytes > 0 && worstCaseSizeInBytes > splittableIfLargerThanBytes) {
            return true;
        }
        if (splittableIfKeysLargerThanBytes > 0 && worstCaseKeysSizeInBytes > splittableIfKeysLargerThanBytes) {
            return true;
        }
        return splittableIfValuesLargerThanBytes > 0 && worstCaseValuesSizeInBytes > splittableIfValuesLargerThanBytes;
    }

    private Void buildSplitter(IndexFactory leftHalfIndexFactory,
        IndexFactory rightHalfIndexFactory,
        CommitIndex commitIndex,
        boolean fsync) throws Exception {

        // lock out merging if possible
        long allVersion;
        RawConcurrentReadableIndex[] all;
        synchronized (indexesLock) {
            allVersion = version;
            for (boolean b : merging) {
                if (b) {
                    return null;
                }
            }
            Arrays.fill(merging, true);
            all = indexes;
        }
        return new Splitter(all, allVersion, leftHalfIndexFactory, rightHalfIndexFactory, commitIndex, fsync).call();

    }

    public class Splitter implements Callable<Void> {

        private final RawConcurrentReadableIndex[] all;
        private long allVersion;
        private final IndexFactory leftHalfIndexFactory;
        private final IndexFactory rightHalfIndexFactory;
        private final CommitIndex commitIndex;
        private final boolean fsync;

        public Splitter(RawConcurrentReadableIndex[] all,
            long allVersion,
            IndexFactory leftHalfIndexFactory,
            IndexFactory rightHalfIndexFactory,
            CommitIndex commitIndex,
            boolean fsync) {

            this.all = all;
            this.allVersion = allVersion;
            this.leftHalfIndexFactory = leftHalfIndexFactory;
            this.rightHalfIndexFactory = rightHalfIndexFactory;
            this.commitIndex = commitIndex;
            this.fsync = fsync;
        }

        @Override
        public Void call() throws Exception {
            while (true) {
                ReadIndex[] readers = new ReadIndex[all.length];
                try {
                    int splitLength = all.length;
                    long worstCaseCount = 0;
                    NextRawEntry[] feeders = new NextRawEntry[all.length];
                    IndexRangeId join = null;
                    byte[] minKey = null;
                    byte[] maxKey = null;
                    for (int i = 0; i < all.length; i++) {
                        readers[i] = all[i].acquireReader();
                        worstCaseCount += readers[i].count();
                        feeders[i] = readers[i].rowScan();
                        IndexRangeId id = all[i].id();
                        if (join == null) {
                            join = new IndexRangeId(id.start, id.end, id.generation + 1);
                        } else {
                            join = join.join(id, Math.max(join.generation, id.generation + 1));
                        }

                        if (minKey == null) {
                            minKey = all[i].minKey();
                        } else {
                            minKey = UnsignedBytes.lexicographicalComparator().compare(minKey, all[i].minKey()) < 0 ? minKey : all[i].minKey();
                        }

                        if (maxKey == null) {
                            maxKey = all[i].maxKey();
                        } else {
                            maxKey = UnsignedBytes.lexicographicalComparator().compare(maxKey, all[i].maxKey()) < 0 ? maxKey : all[i].maxKey();
                        }
                    }

                    if (Arrays.equals(minKey, maxKey)) {
                        // TODO how not to get here over an over again when a key is larger that split size in byte Cannot split a single key
                        LOG.warn("Trying to split a single key." + Arrays.toString(minKey));
                        return null;
                    } else {
                        byte[] middle = Lists.newArrayList(UIO.iterateOnSplits(minKey, maxKey, true, 1)).get(1);
                        LABAppendableIndex leftAppenableIndex = null;
                        LABAppendableIndex rightAppenableIndex = null;
                        try {
                            leftAppenableIndex = leftHalfIndexFactory.createIndex(join, worstCaseCount - 1);
                            rightAppenableIndex = rightHalfIndexFactory.createIndex(join, worstCaseCount - 1);
                            LABAppendableIndex effectiveFinalRightAppenableIndex = rightAppenableIndex;
                            InterleaveStream feedInterleaver = new InterleaveStream(feeders, rawhide);

                            LOG.debug("Splitting with a middle of:{}", Arrays.toString(middle));
                            leftAppenableIndex.append((leftStream) -> {
                                return effectiveFinalRightAppenableIndex.append((rightStream) -> {
                                    return feedInterleaver.stream((rawEntry, offset, length) -> {
                                        int c = rawhide.compareKey(rawEntry, offset, middle, 0, middle.length);
                                        if (c < 0) {
                                            if (!leftStream.stream(rawEntry, offset, length)) {
                                                return false;
                                            }
                                        } else if (!rightStream.stream(rawEntry, offset, length)) {
                                            return false;
                                        }
                                        return true;
                                    });
                                });
                            });

                            LOG.debug("Splitting is flushing for a middle of:{}", Arrays.toString(middle));
                            leftAppenableIndex.closeAppendable(fsync);
                            rightAppenableIndex.closeAppendable(fsync);
                        } catch (Exception x) {
                            try {
                                if (leftAppenableIndex != null) {
                                    leftAppenableIndex.getIndex().close();
                                    leftAppenableIndex.getIndex().getFile().delete();
                                }
                                if (rightAppenableIndex != null) {
                                    rightAppenableIndex.getIndex().close();
                                    rightAppenableIndex.getIndex().getFile().delete();
                                }
                            } catch (Exception xx) {
                                LOG.error("Failed while trying to cleanup after a failure.", xx);
                            }
                            throw x;
                        }

                        List<IndexRangeId> commitRanges = new ArrayList<>();
                        commitRanges.add(join);

                        LOG.debug("Splitting trying to catchup for a middle of:{}", Arrays.toString(middle));
                        CATCHUP_YOU_BABY_TOMATO:
                        while (true) {
                            RawConcurrentReadableIndex[] catchupMergeSet;
                            synchronized (indexesLock) {
                                if (allVersion == version) {
                                    LOG.debug("Commiting split for a middle of:{}", Arrays.toString(middle));

                                    commitIndex.commit(commitRanges);
                                    for (RawConcurrentReadableIndex destroy : all) {
                                        destroy.destroy();
                                    }
                                    version++;
                                    indexes = new RawConcurrentReadableIndex[0]; // TODO go handle null so that thread wait rety higher up
                                    refreshMaxTimestamp(indexes);
                                    merging = new boolean[0];
                                    disposed = true;
                                    LOG.debug("All done splitting :) for a middle of:{}", Arrays.toString(middle));
                                    return null;
                                } else {

                                    LOG.debug("Version has changed {} for a middle of:{}", allVersion, Arrays.toString(middle));
                                    int catchupLength = merging.length - splitLength;
                                    for (int i = 0; i < catchupLength; i++) {
                                        if (merging[i]) {
                                            LOG.debug("Waiting for merge flag to clear at {} for a middle of:{}", i, Arrays.toString(middle));
                                            LOG.debug("splitLength={} merge.length={} catchupLength={}", splitLength, merging.length, catchupLength);
                                            LOG.debug("merging:{}", Arrays.toString(merging));
                                            indexesLock.wait();
                                            LOG.debug("Merge flag to cleared at {} for a middle of:{}", i, Arrays.toString(middle));
                                            continue CATCHUP_YOU_BABY_TOMATO;
                                        }
                                    }
                                    allVersion = version;
                                    catchupMergeSet = new RawConcurrentReadableIndex[catchupLength];
                                    Arrays.fill(merging, 0, catchupLength, true);
                                    System.arraycopy(indexes, 0, catchupMergeSet, 0, catchupLength);
                                    splitLength = merging.length;
                                }
                            }

                            for (RawConcurrentReadableIndex catchup : catchupMergeSet) {
                                IndexRangeId id = catchup.id();

                                LABAppendableIndex catupLeftAppenableIndex = null;
                                LABAppendableIndex catchupRightAppenableIndex = null;
                                try {
                                    catupLeftAppenableIndex = leftHalfIndexFactory.createIndex(id, catchup.count());
                                    catchupRightAppenableIndex = rightHalfIndexFactory.createIndex(id, catchup.count());
                                    LABAppendableIndex effectivelyFinalCatchupRightAppenableIndex = catchupRightAppenableIndex;

                                    ReadIndex catchupReader = catchup.acquireReader();
                                    try {
                                        InterleaveStream catchupFeedInterleaver = new InterleaveStream(new NextRawEntry[]{catchupReader.rowScan()}, rawhide);

                                        LOG.debug("Doing a catchup split for a middle of:{}", Arrays.toString(middle));
                                        catupLeftAppenableIndex.append((leftStream) -> {
                                            return effectivelyFinalCatchupRightAppenableIndex.append((rightStream) -> {
                                                return catchupFeedInterleaver.stream((rawEntry, offset, length) -> {
                                                    if (UnsignedBytes.lexicographicalComparator().compare(rawEntry, middle) < 0) {
                                                        if (!leftStream.stream(rawEntry, offset, length)) {
                                                            return false;
                                                        }
                                                    } else if (!rightStream.stream(rawEntry, offset, length)) {
                                                        return false;
                                                    }
                                                    return true;
                                                });
                                            });
                                        });
                                    } finally {
                                        catchupReader.release();
                                    }
                                    LOG.debug("Catchup splitting is flushing for a middle of:{}", Arrays.toString(middle));
                                    catupLeftAppenableIndex.closeAppendable(fsync);
                                    catchupRightAppenableIndex.closeAppendable(fsync);

                                    commitRanges.add(0, id);

                                } catch (Exception x) {
                                    try {
                                        if (catupLeftAppenableIndex != null) {
                                            catupLeftAppenableIndex.getIndex().close();
                                            catupLeftAppenableIndex.getIndex().getFile().delete();
                                        }
                                        if (catchupRightAppenableIndex != null) {
                                            catchupRightAppenableIndex.getIndex().close();
                                            catchupRightAppenableIndex.getIndex().getFile().delete();
                                        }
                                    } catch (Exception xx) {
                                        LOG.error("Failed while trying to cleanup after a failure.", xx);
                                    }
                                    throw x;
                                }
                            }
                        }
                    }

                } catch (Exception x) {
                    synchronized (indexesLock) {
                        Arrays.fill(merging, false);
                    }
                    throw x;
                } finally {
                    for (ReadIndex reader : readers) {
                        if (reader != null) {
                            reader.release();
                        }
                    }
                }
            }
        }
    }

    private Merger buildMerger(int minimumRun, boolean fsync, IndexFactory indexFactory, CommitIndex commitIndex) throws Exception {
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

    public class Merger implements Callable<Void> {

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
        public Void call() throws Exception {
            LeapsAndBoundsIndex index = null;
            ReadIndex[] readers = new ReadIndex[mergeSet.length];
            try {

                long startMerge = System.currentTimeMillis();

                long worstCaseCount = 0;
                NextRawEntry[] feeders = new NextRawEntry[mergeSet.length];
                for (int i = 0; i < feeders.length; i++) {
                    readers[i] = mergeSet[i].acquireReader();
                    worstCaseCount += readers[i].count();
                    feeders[i] = readers[i].rowScan();
                }

                LABAppendableIndex appenableIndex = null;
                try {
                    appenableIndex = indexFactory.createIndex(mergeRangeId, worstCaseCount);
                    InterleaveStream feedInterleaver = new InterleaveStream(feeders, rawhide);
                    appenableIndex.append((stream) -> {
                        return feedInterleaver.stream(stream);
                    });
                    appenableIndex.closeAppendable(fsync);
                } catch (Exception x) {
                    try {
                        if (appenableIndex != null) {
                            appenableIndex.getIndex().close();
                            appenableIndex.getIndex().getFile().delete();
                        }
                    } catch (Exception xx) {
                        LOG.error("Failed while trying to cleanup after a failure.", xx);
                    }
                    throw x;
                }

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
                    refreshMaxTimestamp(updateIndexes);
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
            } finally {
                for (ReadIndex reader : readers) {
                    if (reader != null) {
                        reader.release();
                    }
                }
            }

            return null;
        }

    }

    public boolean tx(int index, byte[] fromKey, byte[] toKey, ReaderTx tx) throws Exception {

        RawConcurrentReadableIndex[] stackIndexes;

        ReadIndex[] readIndexs = null;
        START_OVER:
        while (true) {
            synchronized (indexesLock) {
                stackIndexes = indexes;
            }
            readIndexs = new ReadIndex[stackIndexes.length];
            try {
                for (int i = 0; i < readIndexs.length; i++) {
                    readIndexs[i] = stackIndexes[i].acquireReader();
                    if (readIndexs[i] == null) {
                        releaseReaders(readIndexs);
                        continue START_OVER;
                    }
                }
            } catch (Exception x) {
                releaseReaders(readIndexs);
                throw x;
            }
            break;
        }

        try {
            return tx.tx(index, fromKey, toKey, readIndexs);
        } finally {
            releaseReaders(readIndexs);
        }
    }

    private void releaseReaders(ReadIndex[] readIndexs) {
        for (int i = 0; i < readIndexs.length; i++) {
            if (readIndexs[i] != null) {
                readIndexs[i].release();
                readIndexs[i] = null;
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

    public void destroy() throws Exception {
        synchronized (indexesLock) {
            for (RawConcurrentReadableIndex index : indexes) {
                index.destroy();
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
