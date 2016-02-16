package com.jivesoftware.os.lab;

import com.jivesoftware.os.lab.api.LABIndexCorruptedException;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.api.Values;
import com.jivesoftware.os.lab.guts.IndexUtil;
import com.jivesoftware.os.lab.guts.RangeStripedCompactableIndexes;
import com.jivesoftware.os.lab.guts.RawMemoryIndex;
import com.jivesoftware.os.lab.guts.ReaderTx;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.NextRawEntry;
import com.jivesoftware.os.lab.guts.api.NextRawEntry.Next;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.io.AppenableHeap;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author jonathan.colt
 */
public class LAB implements ValueIndex {

    static private class CommitLock {
    }

    static private class CompactLock {
    }

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final ExecutorService destroy;
    private final ExecutorService compact;
    private final int maxUpdatesBeforeFlush;
    private final int minDebt;
    private final int maxDebt;
    private final RangeStripedCompactableIndexes rangeStripedCompactableIndexes;
    private final CommitLock commitLock = new CommitLock();
    private final CompactLock compactLock = new CompactLock();
    private final AtomicLong ongoingCompactions = new AtomicLong();

    private volatile RawMemoryIndex memoryIndex;
    private volatile RawMemoryIndex flushingMemoryIndex;
    private volatile boolean corrrupt = false;

    private final LABValueMerger valueMerger = new LABValueMerger();

    public LAB(LABValueMerger valueMerger,
        ExecutorService compact,
        ExecutorService destroy,
        File root,
        boolean useMemMap,
        int entriesBetweenLeaps,
        int maxUpdatesBeforeFlush,
        int minDebt,
        int maxDebt,
        long splitWhenKeysTotalExceedsNBytes,
        long splitWhenValuesTotalExceedsNBytes,
        long splitWhenValuesAndKeysTotalExceedsNBytes,
        int concurrency) throws Exception {

        this.compact = compact;
        this.destroy = destroy;
        this.maxUpdatesBeforeFlush = maxUpdatesBeforeFlush;
        this.memoryIndex = new RawMemoryIndex(destroy, valueMerger);
        this.rangeStripedCompactableIndexes = new RangeStripedCompactableIndexes(destroy,
            root,
            useMemMap,
            entriesBetweenLeaps,
            splitWhenKeysTotalExceedsNBytes,
            splitWhenValuesTotalExceedsNBytes,
            splitWhenValuesAndKeysTotalExceedsNBytes,
            concurrency);
        this.minDebt = minDebt;
        this.maxDebt = maxDebt;

    } // descending

    @Override
    public int debt() throws Exception {
        return rangeStripedCompactableIndexes.debt(minDebt);
    }

    /*
    public <R> R get(byte[] from, to, KeyStream keys, ValueTx<R> tx) throws Exception {
        // TODO multi get keys
    }
     */
    @Override
    public boolean get(byte[] key, ValueStream stream) throws Exception {
        return tx(key, key, (readIndexes) -> {
            GetRaw getRaw = IndexUtil.get(readIndexes);
            return rawToReal(key, getRaw, stream);
        });
    }

    @Override
    public boolean rangeScan(byte[] from, byte[] to, ValueStream stream) throws Exception {
        return tx(from, to, (readIndexes) -> {
            return rawToReal(IndexUtil.rangeScan(readIndexes, from, to), stream);
        });
    }

    @Override
    public boolean rowScan(ValueStream stream) throws Exception {
        return tx(null, null, (readIndexes) -> {
            return rawToReal(IndexUtil.rowScan(readIndexes), stream);
        });
    }

    @Override
    public void close() throws Exception {
        memoryIndex.closeReadable();
        rangeStripedCompactableIndexes.close();
    }

    @Override
    public long count() throws Exception {
        return memoryIndex.count() + rangeStripedCompactableIndexes.count();
    }

    @Override
    public boolean isEmpty() throws Exception {
        if (memoryIndex.isEmpty()) {
            return rangeStripedCompactableIndexes.isEmpty();
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
                            memoryIndexReader = null;
                        }
                    } else {
                        break;
                    }
                }
            }

            ReadIndex reader = memoryIndexReader;
            ReadIndex flushingReader = flushingMemoryIndexReader;
            return rangeStripedCompactableIndexes.tx(from, to, acquired -> {

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
    public List<Future<Object>> commit(boolean fsync) throws Exception {
        if (corrrupt) {
            throw new LABIndexCorruptedException();
        }
        synchronized (commitLock) {
            if (memoryIndex.isEmpty()) {
                return Collections.emptyList();
            }
            RawMemoryIndex stackCopy = memoryIndex;
            flushingMemoryIndex = stackCopy;
            memoryIndex = new RawMemoryIndex(destroy, valueMerger);
            rangeStripedCompactableIndexes.append(stackCopy, fsync);
            flushingMemoryIndex = null;
            stackCopy.destroy();
        }

        return compact(fsync);
    }

    public List<Future<Object>> compact(boolean fsync) throws Exception {

        int debt = rangeStripedCompactableIndexes.debt(minDebt);
        if (debt == 0) {
            return Collections.emptyList();
        }

        List<Future<Object>> awaitable = null;
        while (true) {
            if (corrrupt) {
                throw new LABIndexCorruptedException();
            }
            List<Callable<Void>> compactors = rangeStripedCompactableIndexes.buildCompactors(minDebt, fsync);
            if (compactors != null && !compactors.isEmpty()) {
                if (awaitable == null) {
                    awaitable = new ArrayList<>(compactors.size());
                }
                for (Callable<Void> compactor : compactors) {
                    LOG.info("Scheduling async compaction:{} for index:{} debt:{}", compactors, rangeStripedCompactableIndexes, debt);
                    ongoingCompactions.incrementAndGet();
                    Future<Object> future = compact.submit(() -> {
                        try {
                            compactor.call();
                        } catch (Exception x) {
                            LOG.error("Failed to compact " + rangeStripedCompactableIndexes, x);
                            corrrupt = true;
                        } finally {
                            synchronized (compactLock) {
                                ongoingCompactions.decrementAndGet();
                                compactLock.notifyAll();
                            }
                        }
                        return null;
                    });
                    awaitable.add(future);
                }
            }

            if (debt >= maxDebt) {
                synchronized (compactLock) {
                    if (ongoingCompactions.get() > 0) {
                        LOG.debug("Waiting because debt it do high index:{} debt:{}", rangeStripedCompactableIndexes, debt);
                        compactLock.wait();
                    } else {
                        return awaitable;
                    }
                }
                debt = rangeStripedCompactableIndexes.debt(minDebt);
            } else {
                return awaitable;
            }
        }
    }

    @Override
    public String toString() {
        return "LAB{"
            + "maxUpdatesBeforeFlush=" + maxUpdatesBeforeFlush
            + ", minDebt=" + minDebt
            + ", maxDebt=" + maxDebt
            + ", ongoingCompactions=" + ongoingCompactions
            + ", corrrupt=" + corrrupt
            + ", valueMerger=" + valueMerger
            + '}';
    }

    private static boolean rawToReal(byte[] key, GetRaw getRaw, ValueStream valueStream) throws Exception {
        return getRaw.get(key, (rawEntry, offset, length) -> streamRawEntry(valueStream, rawEntry, offset));
    }

    private static boolean rawToReal(NextRawEntry nextRawEntry, ValueStream valueStream) throws Exception {
        while (true) {
            Next next = nextRawEntry.next((rawEntry, offset, length) -> {
                return streamRawEntry(valueStream, rawEntry, offset);
            });
            if (next == Next.stopped) {
                return false;
            } else if (next == Next.eos) {
                return true;
            }
        }
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
