package com.jivesoftware.os.lab;

import com.jivesoftware.os.lab.api.LABIndexCorruptedException;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.api.Values;
import com.jivesoftware.os.lab.guts.IndexUtil;
import com.jivesoftware.os.lab.guts.MergeableIndexes;
import com.jivesoftware.os.lab.guts.RangeStripedMergableIndexes;
import com.jivesoftware.os.lab.guts.RawMemoryIndex;
import com.jivesoftware.os.lab.guts.ReaderTx;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.NextRawEntry;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.io.AppenableHeap;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author jonathan.colt
 */
public class LAB implements ValueIndex {

    static private class CommitLock {
    }

    static private class MergeLock {
    }

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ExecutorService destroy;
    private final ExecutorService merge;
    private final int maxUpdatesBeforeFlush;
    private final int minMergeDebt;
    private final int maxMergeDebt;
    private final RangeStripedMergableIndexes stripedMergableIndexes;
    private final CommitLock commitLock = new CommitLock();
    private final MergeLock mergeLock = new MergeLock();
    private final AtomicLong ongoingMerges = new AtomicLong();

    private volatile RawMemoryIndex memoryIndex;
    private volatile RawMemoryIndex flushingMemoryIndex;
    private volatile boolean corrrupt = false;

    private final LABValueMerger valueMerger = new LABValueMerger();

    public LAB(LABValueMerger valueMerger,
        ExecutorService merge,
        ExecutorService destroy,
        File root,
        boolean useMemMap,
        int maxUpdatesBeforeFlush,
        int minMergeDebt,
        int maxMergeDebt,
        long splitWhenKeysTotalExceedsNBytes,
        long splitWhenValuesTotalExceedsNBytes,
        long splitWhenValuesAndKeysTotalExceedsNBytes) throws Exception {

        this.merge = merge;
        this.destroy = destroy;
        this.maxUpdatesBeforeFlush = maxUpdatesBeforeFlush;
        this.memoryIndex = new RawMemoryIndex(destroy, valueMerger);
        this.stripedMergableIndexes = new RangeStripedMergableIndexes(destroy, root, useMemMap, splitWhenKeysTotalExceedsNBytes,
            splitWhenValuesTotalExceedsNBytes, splitWhenValuesAndKeysTotalExceedsNBytes);
        this.minMergeDebt = minMergeDebt;
        this.maxMergeDebt = maxMergeDebt;

    } // descending

    @Override
    public int debt() throws Exception {
        return stripedMergableIndexes.hasMergeDebt(minMergeDebt);
    }

    /*
    public <R> R get(byte[] from, to, KeyStream keys, ValueTx<R> tx) throws Exception {
        // TODO multi get keys
    }
     */
    @Override
    public boolean get(byte[] key, ValueTx tx) throws Exception {
        return tx(key, key, (readIndexes) -> {
            GetRaw getRaw = IndexUtil.get(readIndexes);
            return rawToReal(key, getRaw, tx);
        });
    }

    @Override
    public boolean rangeScan(byte[] from, byte[] to, ValueTx tx) throws Exception {
        return tx(from, to, (readIndexes) -> {
            return rawToReal(IndexUtil.rangeScan(readIndexes, from, to), tx);
        });
    }

    @Override
    public boolean rowScan(ValueTx tx) throws Exception {
        return tx(null, null, (readIndexes) -> {
            return rawToReal(IndexUtil.rowScan(readIndexes), tx);
        });
    }

    @Override
    public void close() throws Exception {
        memoryIndex.closeReadable();
        stripedMergableIndexes.close();
    }

    @Override
    public long count() throws Exception {
        return memoryIndex.count() + stripedMergableIndexes.count();
    }

    @Override
    public boolean isEmpty() throws Exception {
        if (memoryIndex.isEmpty()) {
            return stripedMergableIndexes.isEmpty();
        }
        return false;
    }

    @Override
    public boolean append(Values values) throws Exception {
        long[] count = {memoryIndex.count()};

        boolean appended = memoryIndex.append((stream) -> {
            return values.consume((key, timestamp, tombstoned, version, value) -> {
                count[0]++;
                if (count[0] > maxUpdatesBeforeFlush) { //  TODO flush on memory pressure.
                    count[0] = memoryIndex.count();
                    if (count[0] > maxUpdatesBeforeFlush) { //  TODO flush on memory pressure.
                        commit(true); // TODO hmmm
                    }
                }
                byte[] rawEntry = toRawEntry(key, timestamp, tombstoned, version, value);
                return stream.stream(rawEntry, 0, rawEntry.length);
            });
        });
        return appended;
    }

    private boolean tx(byte[] from, byte[] to, ReaderTx tx) throws Exception {

        ReadIndex memoryIndexReader = null;
        ReadIndex flushingMemoryIndexReader = null;
        try {
            while (true) {
                RawMemoryIndex memoryIndexStackCopy;
                RawMemoryIndex flushingMemoryIndexStackCopy;
                synchronized (commitLock) {
                    memoryIndexStackCopy = memoryIndex;
                    flushingMemoryIndexStackCopy = flushingMemoryIndex;
                }

                memoryIndexReader = memoryIndexStackCopy.reader();
                if (memoryIndexReader != null && memoryIndexReader.acquire()) {

                    if (flushingMemoryIndexStackCopy != null) {
                        flushingMemoryIndexReader = flushingMemoryIndexStackCopy.reader();
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
            return stripedMergableIndexes.tx(from, to, acquired -> {

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
            if (memoryIndex.isEmpty()) {
                return;
            }
            RawMemoryIndex stackCopy = memoryIndex;
            flushingMemoryIndex = stackCopy;
            memoryIndex = new RawMemoryIndex(destroy, valueMerger);
            stripedMergableIndexes.append(stackCopy, fsync);
            flushingMemoryIndex = null;
            stackCopy.destroy();
        }

        merge(fsync);
    }

    public void merge(boolean fsync) throws Exception {

        int mergeDebt = stripedMergableIndexes.hasMergeDebt(minMergeDebt);
        if (mergeDebt <= minMergeDebt) {
            return;
        }

        while (true) {
            if (corrrupt) {
                throw new LABIndexCorruptedException();
            }
            List<MergeableIndexes.Merger> mergers;
            synchronized (mergeLock) {
                mergers = stripedMergableIndexes.buildMerger(minMergeDebt, fsync);

            }
            if (!mergers.isEmpty()) {
                for (MergeableIndexes.Merger merger : mergers) {
                    LOG.info("Scheduling async merger:{} for index:{} debt:{}", mergers, stripedMergableIndexes, mergeDebt);
                    ongoingMerges.incrementAndGet();
                    merge.submit(() -> {
                        try {
                            merger.call();
                            synchronized (mergeLock) {
                                mergeLock.notifyAll();
                            }
                            LAB.this.merge(fsync);
                        } catch (Exception x) {
                            LOG.error("Failed to merge " + stripedMergableIndexes, x);
                            corrrupt = true;
                        } finally {
                            ongoingMerges.decrementAndGet();
                        }
                        return null;
                    });
                }

            }

            if (mergeDebt >= maxMergeDebt) {
                synchronized (mergeLock) {
                    if (ongoingMerges.get() > 0) {
                        LOG.debug("Waiting because merge debt it do high index:{} debt:{}", stripedMergableIndexes, mergeDebt);
                        mergeLock.wait();
                    } else {
                        return;
                    }
                }
                mergeDebt = stripedMergableIndexes.hasMergeDebt(minMergeDebt);
            } else {
                return;
            }
        }
    }

    @Override
    public String toString() {
        return "LAB{"
            + "maxUpdatesBeforeFlush=" + maxUpdatesBeforeFlush
            + ", minMergeDebt=" + minMergeDebt
            + ", maxMergeDebt=" + maxMergeDebt
            + ", ongoingMerges=" + ongoingMerges
            + ", corrrupt=" + corrrupt
            + ", valueMerger=" + valueMerger
            + '}';
    }

    private static boolean rawToReal(byte[] key, GetRaw getRaw, ValueTx tx) throws Exception {
        return tx.tx((stream) -> getRaw.get(key, (rawEntry, offset, length) -> {
            return streamRawEntry(stream, rawEntry, offset);
        }));
    }

    private static boolean rawToReal(NextRawEntry nextRawEntry, ValueTx tx) throws Exception {
        return tx.tx((stream) -> {
            while (true) {
                boolean more = nextRawEntry.next((rawEntry, offset, length) -> {
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
        int o = offset;
        int keyLength = UIO.bytesInt(rawEntry, o);
        o += 4;
        byte[] k = new byte[keyLength];
        System.arraycopy(rawEntry, o, k, 0, keyLength);
        o += keyLength;
        long timestamp = UIO.bytesLong(rawEntry, o);
        o += 8;
        boolean tombstone = rawEntry[o] != 0;
        if (tombstone) {
            return stream.stream(null, -1, false, -1, null);
        }
        o++;
        long version = UIO.bytesLong(rawEntry, o);
        o += 8;

        int payloadLength = UIO.bytesInt(rawEntry, o);
        o += 4;
        byte[] payload = new byte[payloadLength];
        System.arraycopy(rawEntry, o, payload, 0, payloadLength);
        o += payloadLength;

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
