package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.guts.api.CommitIndex;
import com.jivesoftware.os.lab.guts.api.IndexFactory;
import com.jivesoftware.os.lab.guts.api.NextRawEntry;
import com.jivesoftware.os.lab.guts.api.RawConcurrentReadableIndex;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Callable;

/**
 * @author jonathan.colt
 */
public class MergeableIndexes {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    // newest to oldest
    private final Object indexesLock = new Object();
    private volatile boolean[] merging = new boolean[0];
    private volatile RawConcurrentReadableIndex[] indexes = new RawConcurrentReadableIndex[0];
    private volatile long version;

    public void append(RawConcurrentReadableIndex pointerIndex) {
        synchronized (indexesLock) {
            int length = indexes.length + 1;
            boolean[] prependToMerging = new boolean[length];
            prependToMerging[0] = false;
            System.arraycopy(merging, 0, prependToMerging, 1, merging.length);

            RawConcurrentReadableIndex[] prependToIndexes = new RawConcurrentReadableIndex[length];
            prependToIndexes[0] = pointerIndex;
            System.arraycopy(indexes, 0, prependToIndexes, 1, indexes.length);

            merging = prependToMerging;
            indexes = prependToIndexes;
            version++;
        }
    }

    public int hasMergeDebt() throws IOException {
        boolean[] mergingCopy;
        RawConcurrentReadableIndex[] indexesCopy;
        synchronized (indexesLock) {
            mergingCopy = Arrays.copyOf(merging, merging.length);
            indexesCopy = indexes;
        }

        long[] counts = new long[indexesCopy.length];
        long[] generations = new long[indexesCopy.length];
        for (int i = 0; i < counts.length; i++) {
            counts[i] = indexesCopy[i].count();
            generations[i] = indexesCopy[i].id().generation;
        }

        int debt = 0;
        while (true) {
            MergeRange mergeRange = TieredCompaction.getMergeRange(mergingCopy, counts, generations);
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
        }
    }

    public Merger buildMerger(IndexFactory indexFactory, CommitIndex commitIndex) throws Exception {
        boolean[] mergingCopy;
        RawConcurrentReadableIndex[] indexesCopy;
        RawConcurrentReadableIndex[] mergeSet;
        MergeRange mergeRange;
        long[] counts;
        long[] generations;
        synchronized (indexesLock) { // prevent others from trying to merge the same things

            if (indexes.length <= 1) {
                return null;
            }

            mergingCopy = merging;
            indexesCopy = indexes;

            counts = new long[indexesCopy.length];
            generations = new long[indexesCopy.length];
            for (int i = 0; i < counts.length; i++) {
                counts[i] = indexesCopy[i].count();
                generations[i] = indexesCopy[i].id().generation;
            }

            mergeRange = TieredCompaction.getMergeRange(mergingCopy, counts, generations);
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
                join = new IndexRangeId(id.start, id.end, id.generation + 1);
            } else {
                join = join.join(id, Math.max(join.generation, id.generation));
            }
        }

        return new Merger(mergeRange, counts, generations, mergeSet, join, indexFactory, commitIndex);
    }

    public class Merger implements Callable<LeapsAndBoundsIndex> {

        private final MergeRange mergeRange;
        private final long[] counts;
        private final long[] generations;
        private final RawConcurrentReadableIndex[] mergeSet;
        private final IndexRangeId mergeRangeId;
        private final IndexFactory indexFactory;
        private final CommitIndex commitIndex;

        private Merger(MergeRange mergeRange,
            long[] counts,
            long[] generations,
            RawConcurrentReadableIndex[] mergeSet,
            IndexRangeId mergeRangeId,
            IndexFactory indexFactory,
            CommitIndex commitIndex) {

            this.mergeRange = mergeRange;
            this.counts = counts;
            this.generations = generations;
            this.mergeSet = mergeSet;
            this.mergeRangeId = mergeRangeId;
            this.indexFactory = indexFactory;
            this.commitIndex = commitIndex;
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

                WriteLeapsAndBoundsIndex mergedIndex = indexFactory.createIndex(mergeRangeId, worstCaseCount);
                InterleaveStream feedInterleaver = new InterleaveStream(feeders);
                mergedIndex.append((stream) -> {
                    while (feedInterleaver.next(stream)) ;
                    return true;
                });
                mergedIndex.close();

                index = commitIndex.commit(mergeRangeId, mergedIndex);

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

    // For testing :(
    public final Reader reader() throws Exception {
        return new Reader();
    }

    public class Reader {

        private long cacheVersion = -1;
        private RawConcurrentReadableIndex[] stackIndexes;
        private ReadIndex[] readIndexs;

        public <R> R tx(ReaderTx<R> tx) throws Exception {
            TRY_AGAIN:
            while (true) {
                if (cacheVersion < version) {
                    readIndexs = acquireReadIndexes();
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

        private ReadIndex[] acquireReadIndexes() throws Exception {
            TRY_AGAIN:
            while (true) {
                long stackVersion = version;
                stackIndexes = indexes;
                cacheVersion = stackVersion;
                ReadIndex[] readIndexs = new ReadIndex[stackIndexes.length];
                for (int i = 0; i < readIndexs.length; i++) {
                    readIndexs[i] = stackIndexes[i].reader();
                    if (readIndexs[i] == null) {
                        continue TRY_AGAIN;
                    }
                }
                return readIndexs;
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
            for (RawConcurrentReadableIndex indexe : indexes) {
                indexe.close();
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
