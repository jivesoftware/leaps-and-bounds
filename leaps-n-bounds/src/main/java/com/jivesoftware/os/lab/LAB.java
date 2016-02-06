package com.jivesoftware.os.lab;

import com.jivesoftware.os.lab.api.LABIndexCorruptedException;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.api.Values;
import com.jivesoftware.os.lab.guts.IndexFile;
import com.jivesoftware.os.lab.guts.IndexRangeId;
import com.jivesoftware.os.lab.guts.IndexUtil;
import com.jivesoftware.os.lab.guts.LeapsAndBoundsIndex;
import com.jivesoftware.os.lab.guts.MergeableIndexes;
import com.jivesoftware.os.lab.guts.RawMemoryIndex;
import com.jivesoftware.os.lab.guts.ReaderTx;
import com.jivesoftware.os.lab.guts.WriteLeapsAndBoundsIndex;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.NextRawEntry;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.io.AppenableHeap;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;

/**
 *
 * @author jonathan.colt
 */
public class LAB implements ValueIndex {

    static private class CommitLock {}
    static private class MergeLock {}


    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ExecutorService destroy;
    private final ExecutorService merge;
    private final File mergingRoot;
    private final File commitingRoot;
    private final File indexRoot;
    private final boolean useMemMap;
    private final int maxUpdatesBeforeFlush;
    private final int minMergeDebt;
    private final int maxMergeDebt;
    private final AtomicLong largestIndexId = new AtomicLong();
    private final MergeableIndexes mergeablePointerIndexs;

    private volatile RawMemoryIndex memoryPointerIndex;
    private volatile RawMemoryIndex flushingMemoryPointerIndex;
    private final CommitLock commitLock = new CommitLock();
    private final MergeLock mergeLock = new MergeLock();
    private final AtomicLong ongoingMerges = new AtomicLong();
    private volatile boolean corrrupt = false;

    private final LABValueMerger valueMerger = new LABValueMerger();

    public LAB(LABValueMerger valueMerger,
        ExecutorService merge,
        ExecutorService destroy,
        File root,
        boolean useMemMap,
        int maxUpdatesBeforeFlush,
        int minMergeDebt,
        int maxMergeDebt) throws Exception {

        this.merge = merge;
        this.destroy = destroy;
        this.indexRoot = new File(root, "active");
        this.mergingRoot = new File(root, "merging");
        this.commitingRoot = new File(root, "commiting");
        this.useMemMap = useMemMap;
        this.maxUpdatesBeforeFlush = maxUpdatesBeforeFlush;
        this.memoryPointerIndex = new RawMemoryIndex(destroy, valueMerger);
        this.mergeablePointerIndexs = new MergeableIndexes();
        this.minMergeDebt = minMergeDebt;
        this.maxMergeDebt = maxMergeDebt;

        FileUtils.deleteQuietly(mergingRoot);
        FileUtils.deleteQuietly(commitingRoot);
        FileUtils.forceMkdir(mergingRoot);
        FileUtils.forceMkdir(commitingRoot);

        TreeSet<IndexRangeId> ranges = new TreeSet<>();
        File[] listFiles = indexRoot.listFiles();
        if (listFiles != null) {
            for (File file : listFiles) {
                String rawRange = file.getName();
                String[] range = rawRange.split("-");
                long start = Long.parseLong(range[0]);
                long end = Long.parseLong(range[1]);
                long generation = Long.parseLong(range[2]);

                ranges.add(new IndexRangeId(start, end, generation));
                if (largestIndexId.get() < end) {
                    largestIndexId.set(end);
                }
            }
        }

        IndexRangeId active = null;
        TreeSet<IndexRangeId> remove = new TreeSet<>();
        for (IndexRangeId range : ranges) {
            if (active == null || !active.intersects(range)) {
                active = range;
            } else {
                LOG.info("Destroying index for overlaping range:" + range);
                remove.add(range);
            }
        }

        for (IndexRangeId range : remove) {
            File file = range.toFile(indexRoot);
            FileUtils.deleteQuietly(file);
        }
        ranges.removeAll(remove);

        for (IndexRangeId range : ranges) {
            File file = range.toFile(indexRoot);
            if (file.length() == 0) {
                file.delete();
                continue;
            }
            IndexFile indexFile = new IndexFile(file, "rw", useMemMap);
            LeapsAndBoundsIndex pointerIndex = new LeapsAndBoundsIndex(destroy, range, indexFile);
            mergeablePointerIndexs.append(pointerIndex);
        }
    } // descending

    @Override
    public int debt() throws Exception {
        return mergeablePointerIndexs.hasMergeDebt(minMergeDebt);
    }

    @Override
    public <R> R get(byte[] key, ValueTx<R> tx) throws Exception {
        return tx((readIndexes) -> {
            GetRaw getRaw = IndexUtil.get(readIndexes);
            return rawToReal(key, getRaw, tx);
        });
    }

    @Override
    public <R> R rangeScan(byte[] from, byte[] to, ValueTx<R> tx) throws Exception {
        return tx((readIndexes) -> {
            return rawToReal(IndexUtil.rangeScan(readIndexes, from, to), tx);
        });
    }

    @Override
    public <R> R rowScan(ValueTx<R> tx) throws Exception {
        return tx((readIndexes) -> {
            return rawToReal(IndexUtil.rowScan(readIndexes), tx);
        });
    }

    @Override
    public void close() throws Exception {
        memoryPointerIndex.closeReadable();
        mergeablePointerIndexs.close();
    }

    @Override
    public long count() throws Exception {
        return memoryPointerIndex.count() + mergeablePointerIndexs.count();
    }

    @Override
    public boolean isEmpty() throws Exception {
        if (memoryPointerIndex.isEmpty()) {
            return mergeablePointerIndexs.isEmpty();
        }
        return false;
    }

    @Override
    public boolean append(Values pointers) throws Exception {
        long[] count = {memoryPointerIndex.count()};

        boolean appended = memoryPointerIndex.append((stream) -> {
            return pointers.consume((key, timestamp, tombstoned, version, pointer) -> {
                count[0]++;
                if (count[0] > maxUpdatesBeforeFlush) { //  TODO flush on memory pressure.
                    count[0] = memoryPointerIndex.count();
                    if (count[0] > maxUpdatesBeforeFlush) { //  TODO flush on memory pressure.
                        commit(true); // TODO hmmm
                    }
                }
                byte[] rawEntry = toRawEntry(key, timestamp, tombstoned, version, pointer);
                return stream.stream(rawEntry, 0, rawEntry.length);
            });
        });
        return appended;
    }

    private <R> R tx(ReaderTx<R> tx) throws Exception {
//        return mergeablePointerIndexs.reader().tx(acquired -> {
//            return tx.tx(acquired);
//        });

        ReadIndex memoryIndexReader = null;
        ReadIndex flushingMemoryIndexReader = null;
        try {
            while (true) {
                RawMemoryIndex memoryIndex;
                RawMemoryIndex flushingMemoryIndex;
                synchronized (commitLock) {
                    memoryIndex = memoryPointerIndex;
                    flushingMemoryIndex = flushingMemoryPointerIndex;
                }

                memoryIndexReader = memoryIndex.reader();
                if (memoryIndexReader != null && memoryIndexReader.acquire()) {

                    if (flushingMemoryIndex != null) {
                        flushingMemoryIndexReader = flushingMemoryIndex.reader();
                        if (flushingMemoryIndexReader != null && flushingMemoryIndexReader.acquire()) {
                            break;
                        } else {
                            memoryIndexReader.release();
                        }
                    } else {
                        break;
                    }
                }
            }

            ReadIndex reader = memoryIndexReader;
            ReadIndex flushingReader = flushingMemoryIndexReader;
            return mergeablePointerIndexs.reader().tx(acquired -> {

                int flushing = (flushingReader == null) ? 0 : 1;
                ReadIndex[] indexes = new ReadIndex[acquired.length + 1 + flushing];
                indexes[0] = reader;
                if (flushingReader != null) {
                    indexes[1] = flushingReader;
                }
                System.arraycopy(acquired, 0, indexes, 1 + flushing, acquired.length);
                return tx.tx(indexes);
            });
        } finally {
            if (memoryIndexReader != null) {
                memoryIndexReader.release();
            }
            if (flushingMemoryIndexReader != null) {
                flushingMemoryIndexReader.release();
            }
        }

    }

    @Override
    public void commit(boolean fsync) throws Exception {
        if (corrrupt) {
            throw new LABIndexCorruptedException();
        }
        synchronized (commitLock) {
            if (memoryPointerIndex.isEmpty()) {
                return;
            }
            RawMemoryIndex stackCopy = memoryPointerIndex;
            flushingMemoryPointerIndex = stackCopy;
            memoryPointerIndex = new RawMemoryIndex(destroy, valueMerger);

            LeapsAndBoundsIndex reopenedIndex = flushMemoryIndexToDisk(stackCopy, largestIndexId.incrementAndGet(), 0, fsync);
            flushingMemoryPointerIndex = null;
            mergeablePointerIndexs.append(reopenedIndex);
            stackCopy.destroy();
        }

        merge(fsync);
    }

    private LeapsAndBoundsIndex flushMemoryIndexToDisk(RawMemoryIndex index, long nextIndexId, int generation, boolean fsync) throws Exception {
        LOG.debug("Commiting memory index (" + flushingMemoryPointerIndex.count() + ") to on disk index." + indexRoot);

        int entriesBetweenLeaps = 4096; // TODO expose to a config;
        int maxLeaps = IndexUtil.calculateIdealMaxLeaps(flushingMemoryPointerIndex.count(), entriesBetweenLeaps);
        IndexRangeId indexRangeId = new IndexRangeId(nextIndexId, nextIndexId, generation);
        File commitingIndexFile = indexRangeId.toFile(commitingRoot);
        FileUtils.deleteQuietly(commitingIndexFile);
        IndexFile indexFile = new IndexFile(commitingIndexFile, "rw", useMemMap);
        WriteLeapsAndBoundsIndex write = new WriteLeapsAndBoundsIndex(indexRangeId, indexFile, maxLeaps, entriesBetweenLeaps);
        write.append(index);
        write.closeAppendable(fsync);

        File commitedIndexFile = indexRangeId.toFile(indexRoot);
        FileUtils.moveFile(commitingIndexFile, commitedIndexFile);

        LeapsAndBoundsIndex reopenedIndex = new LeapsAndBoundsIndex(destroy,
            indexRangeId, new IndexFile(commitedIndexFile, "r", useMemMap));
        reopenedIndex.flush(true);  // Sorry
        // TODO Files.fsync index when java 9 supports it.
        return reopenedIndex;
    }

    public void merge(boolean fsync) throws Exception {

        int mergeDebt = mergeablePointerIndexs.hasMergeDebt(minMergeDebt);
        if (mergeDebt <= minMergeDebt) {
            return;
        }

        while (true) {
            if (corrrupt) {
                throw new LABIndexCorruptedException();
            }
            MergeableIndexes.Merger merger;
            synchronized (mergeLock) {
                merger = mergeablePointerIndexs.buildMerger(
                    minMergeDebt,
                    (id, count) -> {

                        int entriesBetweenLeaps = 4096; // TODO expose to a config;
                        int maxLeaps = IndexUtil.calculateIdealMaxLeaps(count, entriesBetweenLeaps);

                        File mergingIndexFile = id.toFile(mergingRoot);
                        FileUtils.deleteQuietly(mergingIndexFile);
                        IndexFile indexFile = new IndexFile(mergingIndexFile, "rw", useMemMap);
                        WriteLeapsAndBoundsIndex writeLeapsAndBoundsIndex = new WriteLeapsAndBoundsIndex(id, indexFile,
                            maxLeaps,
                            entriesBetweenLeaps);
                        return writeLeapsAndBoundsIndex;
                    },
                    (id, index) -> {
                        File mergedIndexFile = id.toFile(mergingRoot);
                        File file = id.toFile(indexRoot);
                        FileUtils.deleteQuietly(file);

                        FileUtils.moveFile(mergedIndexFile, file);
                        IndexFile indexFile = new IndexFile(file, "r", useMemMap);
                        LeapsAndBoundsIndex reopenedIndex = new LeapsAndBoundsIndex(destroy, id, indexFile);
                        reopenedIndex.flush(true); // Sorry
                        // TODO Files.fsync mergedIndexRoot when java 9 supports it.
                        return reopenedIndex;
                    }, fsync);

            }
            if (merger != null) {
                LOG.info("Scheduling async merger:{} for index:{} debt:{}", merger, indexRoot, mergeDebt);
                ongoingMerges.incrementAndGet();
                merge.submit(() -> {
                    try {
                        merger.call();
                        synchronized (mergeLock) {
                            mergeLock.notifyAll();
                        }
                    } catch (Exception x) {
                        LOG.error("Failed to merge " + indexRoot, x);
                        corrrupt = true;
                    } finally {
                        ongoingMerges.decrementAndGet();
                    }
                    return null;
                });

            }

            if (mergeDebt >= maxMergeDebt) {
                synchronized (mergeLock) {
                    if (ongoingMerges.get() > 0) {
                        LOG.debug("Waiting because merge debt it do high index:{} debt:{}", indexRoot, mergeDebt);
                        mergeLock.wait();
                    } else {
                        return;
                    }
                }
                mergeDebt = mergeablePointerIndexs.hasMergeDebt(minMergeDebt);
            } else {
                return;
            }
        }
    }

    @Override
    public String toString() {
        return "LAB{"
            + "indexRoot=" + indexRoot
            + ", useMemMap=" + useMemMap
            + ", maxUpdatesBeforeFlush=" + maxUpdatesBeforeFlush
            + ", minMergeDebt=" + minMergeDebt
            + ", maxMergeDebt=" + maxMergeDebt
            + ", largestIndexId=" + largestIndexId
            + ", ongoingMerges=" + ongoingMerges
            + ", corrrupt=" + corrrupt
            + ", valueMerger=" + valueMerger
            + '}';
    }

    private static <R> R rawToReal(byte[] key, GetRaw rawNextPointer, ValueTx<R> tx) throws Exception {
        return tx.tx((stream) -> rawNextPointer.get(key, (rawEntry, offset, length) -> {
            return streamRawEntry(stream, rawEntry, offset);
        }));
    }

    private static <R> R rawToReal(NextRawEntry rawNextPointer, ValueTx<R> tx) throws Exception {
        return tx.tx((stream) -> {
            while (true) {
                boolean more = rawNextPointer.next((rawEntry, offset, length) -> {
                    return streamRawEntry(stream, rawEntry, offset);
                });
                if (!more) {
                    return false;
                }
            }
        });
    }

    private static boolean streamRawEntry(ValueStream stream, byte[] rawEntry, int offset) throws Exception {
        if (rawEntry == null) {
            return stream.stream(null, -1, false, -1, null);
        }
        int keyLength = UIO.bytesInt(rawEntry, offset);
        offset += 4;
        byte[] k = new byte[keyLength];
        System.arraycopy(rawEntry, offset, k, 0, keyLength);
        offset += keyLength;
        long timestamp = UIO.bytesLong(rawEntry, offset);
        offset += 8;
        boolean tombstone = rawEntry[offset] != 0;
        if (tombstone) {
            return stream.stream(null, -1, false, -1, null);
        }
        offset++;
        long version = UIO.bytesLong(rawEntry, offset);
        offset += 8;

        int payloadLength = UIO.bytesInt(rawEntry, offset);
        offset += 4;
        byte[] payload = new byte[payloadLength];
        System.arraycopy(rawEntry, offset, payload, 0, payloadLength);
        offset += payloadLength;

        return stream.stream(k, timestamp, tombstone, version, payload);
    }

    private static byte[] toRawEntry(byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) throws IOException {

        AppenableHeap indexEntryFiler = new AppenableHeap(4 + key.length + 8 + 1 + 4 + payload.length); // TODO somthing better
        byte[] lengthBuffer = new byte[4];
        UIO.writeByteArray(indexEntryFiler, key, "key", lengthBuffer);
        UIO.writeLong(indexEntryFiler, timestamp, "timestamp");
        UIO.writeByte(indexEntryFiler, tombstoned ? (byte) 1 : (byte) 0, "tombstone");
        UIO.writeLong(indexEntryFiler, version, "version");
        UIO.writeByteArray(indexEntryFiler, payload, "payload", lengthBuffer);
        return indexEntryFiler.getBytes();
    }
}
