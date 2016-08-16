package com.jivesoftware.os.lab;

import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.FormatTransformerProvider;
import com.jivesoftware.os.lab.api.Keys;
import com.jivesoftware.os.lab.api.LABIndexClosedException;
import com.jivesoftware.os.lab.api.LABIndexCorruptedException;
import com.jivesoftware.os.lab.api.Ranges;
import com.jivesoftware.os.lab.api.RawEntryFormat;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.api.Values;
import com.jivesoftware.os.lab.guts.IndexUtil;
import com.jivesoftware.os.lab.guts.Leaps;
import com.jivesoftware.os.lab.guts.RangeStripedCompactableIndexes;
import com.jivesoftware.os.lab.guts.RawMemoryIndex;
import com.jivesoftware.os.lab.guts.ReaderTx;
import com.jivesoftware.os.lab.guts.TimestampAndVersion;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.KeyToString;
import com.jivesoftware.os.lab.guts.api.NextRawEntry;
import com.jivesoftware.os.lab.guts.api.NextRawEntry.Next;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jonathan.colt
 */
public class LAB implements ValueIndex {

    static private class CompactLock {
    }

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    final ExecutorService schedule;
    private final ExecutorService compact;
    private final ExecutorService destroy;
    private final LabWAL wal;
    private final byte[] walId;
    private final AtomicLong walAppendVersion = new AtomicLong(0);

    private final LabHeapPressure labHeapFlusher;
    private final long maxHeapPressureInBytes;
    private final int minDebt;
    private final int maxDebt;
    private final String indexName;
    private final RangeStripedCompactableIndexes rangeStripedCompactableIndexes;
    private final Semaphore commitSemaphore = new Semaphore(Short.MAX_VALUE, true);
    private final CompactLock compactLock = new CompactLock();
    private final AtomicLong ongoingCompactions = new AtomicLong();
    private final AtomicBoolean closeRequested = new AtomicBoolean(false);

    private volatile RawMemoryIndex memoryIndex;
    private volatile RawMemoryIndex flushingMemoryIndex;
    private volatile boolean corrupt = false;

    private final Rawhide rawhide;
    private final AtomicReference<RawEntryFormat> rawEntryFormat;

    public LAB(FormatTransformerProvider formatTransformerProvider,
        Rawhide rawhide,
        RawEntryFormat rawEntryFormat,
        ExecutorService schedule,
        ExecutorService compact,
        ExecutorService destroy,
        File root,
        LabWAL wal,
        byte[] walId,
        String indexName,
        boolean useMemMap,
        int entriesBetweenLeaps,
        LabHeapPressure labHeapFlusher,
        long maxHeapPressureInBytes,
        int minDebt,
        int maxDebt,
        long splitWhenKeysTotalExceedsNBytes,
        long splitWhenValuesTotalExceedsNBytes,
        long splitWhenValuesAndKeysTotalExceedsNBytes,
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache) throws Exception {

        this.rawhide = rawhide;
        this.schedule = schedule;
        this.compact = compact;
        this.destroy = destroy;
        this.wal = wal;
        this.walId = walId;
        this.labHeapFlusher = labHeapFlusher;
        this.maxHeapPressureInBytes = maxHeapPressureInBytes;
        this.memoryIndex = new RawMemoryIndex(destroy, labHeapFlusher.globalHeapCostInBytes(), rawhide);
        this.rawEntryFormat = new AtomicReference<>(rawEntryFormat);
        this.indexName = indexName;
        this.rangeStripedCompactableIndexes = new RangeStripedCompactableIndexes(destroy,
            root,
            indexName,
            useMemMap,
            entriesBetweenLeaps,
            splitWhenKeysTotalExceedsNBytes,
            splitWhenValuesTotalExceedsNBytes,
            splitWhenValuesAndKeysTotalExceedsNBytes,
            formatTransformerProvider,
            rawhide,
            this.rawEntryFormat,
            leapsCache);
        this.minDebt = minDebt;
        this.maxDebt = maxDebt;
    }

    @Override
    public String name() {
        return indexName;
    }

    @Override
    public int debt() throws Exception {
        return rangeStripedCompactableIndexes.debt();
    }

    @Override
    public boolean get(Keys keys, ValueStream stream, boolean hydrateValues) throws Exception {
        int[] count = {0};
        boolean b = pointTx(keys, -1, -1, (index, fromKey, toKey, readIndexes, hydrateValues1) -> {
            GetRaw getRaw = IndexUtil.get(readIndexes);
            count[0]++;
            return rawToReal(index, fromKey, getRaw, stream, hydrateValues1);
        }, hydrateValues);
        LOG.inc("LAB>gets", count[0]);
        return b;
    }

    @Override
    public boolean rangeScan(byte[] from, byte[] to, ValueStream stream, boolean hydrateValues) throws Exception {
        return rangeTx(true, -1, from, to, -1, -1,
            (index, fromKey, toKey, readIndexes, hydrateValues1) -> {
                return rawToReal(index, IndexUtil.rangeScan(readIndexes, fromKey, toKey, rawhide), stream, hydrateValues1);
            },
            hydrateValues
        );
    }

    @Override
    public boolean rangesScan(Ranges ranges, ValueStream stream, boolean hydrateValues) throws Exception {
        return ranges.ranges((int index, byte[] from, byte[] to) -> {
            return rangeTx(true, index, from, to, -1, -1,
                (index1, fromKey, toKey, readIndexes, hydrateValues1) -> {
                    return rawToReal(index1, IndexUtil.rangeScan(readIndexes, fromKey, toKey, rawhide), stream, hydrateValues1);
                },
                hydrateValues
            );
        });

    }

    private final static byte[] smallestPossibleKey = new byte[0];

    @Override
    public boolean rowScan(ValueStream stream, boolean hydrateValues) throws Exception {
        return rangeTx(true, -1, smallestPossibleKey, null, -1, -1,
            (index, fromKey, toKey, readIndexes, hydrateValues1) -> {
                return rawToReal(index, IndexUtil.rangeScan(readIndexes, fromKey, toKey, rawhide), stream, hydrateValues1);
            },
            hydrateValues
        );
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

    public long approximateHeapPressureInBytes() {
        RawMemoryIndex stackCopyFlushingMemoryIndex = flushingMemoryIndex;
        return memoryIndex.sizeInBytes() + ((stackCopyFlushingMemoryIndex == null) ? 0 : stackCopyFlushingMemoryIndex.sizeInBytes());
    }

    private boolean pointTx(Keys keys,
        long newerThanTimestamp,
        long newerThanTimestampVersion,
        ReaderTx tx,
        boolean hydrateValues) throws Exception {

        ReadIndex memoryIndexReader = null;
        ReadIndex flushingMemoryIndexReader = null;
        try {
            while (true) {
                RawMemoryIndex memoryIndexStackCopy;
                RawMemoryIndex flushingMemoryIndexStackCopy;

                commitSemaphore.acquire();
                try {
                    memoryIndexStackCopy = memoryIndex;
                    flushingMemoryIndexStackCopy = flushingMemoryIndex;
                } finally {
                    commitSemaphore.release();
                }

                if (mightContain(memoryIndexStackCopy, newerThanTimestamp, newerThanTimestampVersion)) {

                    memoryIndexReader = memoryIndexStackCopy.acquireReader();
                    if (memoryIndexReader != null) {
                        if (flushingMemoryIndexStackCopy != null) {
                            if (mightContain(flushingMemoryIndexStackCopy, newerThanTimestamp, newerThanTimestampVersion)) {
                                flushingMemoryIndexReader = flushingMemoryIndexStackCopy.acquireReader();
                                if (flushingMemoryIndexReader != null) {
                                    break;
                                } else {
                                    memoryIndexReader.release();
                                    memoryIndexReader = null;
                                }
                            }
                        } else {
                            break;
                        }
                    }
                } else if (flushingMemoryIndexStackCopy != null) {
                    if (mightContain(flushingMemoryIndexStackCopy, newerThanTimestamp, newerThanTimestampVersion)) {
                        flushingMemoryIndexReader = flushingMemoryIndexStackCopy.acquireReader();
                        if (flushingMemoryIndexReader != null) {
                            break;
                        }
                    }
                } else {
                    break;
                }

            }

            ReadIndex reader = memoryIndexReader;
            ReadIndex flushingReader = flushingMemoryIndexReader;
            return rangeStripedCompactableIndexes.pointTx(keys,
                newerThanTimestamp,
                newerThanTimestampVersion,
                (index, fromKey, toKey, acquired, hydrateValues1) -> {
                    int active = (reader == null) ? 0 : 1;
                    int flushing = (flushingReader == null) ? 0 : 1;
                    ReadIndex[] indexes = new ReadIndex[acquired.length + active + flushing];
                    int i = 0;
                    if (reader != null) {
                        indexes[i] = reader;
                        i++;
                    }
                    if (flushingReader != null) {
                        indexes[i] = flushingReader;
                        i++;
                    }
                    System.arraycopy(acquired, 0, indexes, active + flushing, acquired.length);
                    pointTxCalled.incrementAndGet();
                    pointTxIndexCount.addAndGet(indexes.length);
                    return tx.tx(index, fromKey, toKey, indexes, hydrateValues1);
                }, hydrateValues);
        } finally {
            if (memoryIndexReader != null) {
                memoryIndexReader.release();
            }
            if (flushingMemoryIndexReader != null) {
                flushingMemoryIndexReader.release();
            }
        }

    }

    public static final AtomicLong pointTxCalled = new AtomicLong();
    public static final AtomicLong pointTxIndexCount = new AtomicLong();

    private boolean rangeTx(boolean acquireCommitSemaphore,
        int index,
        byte[] from,
        byte[] to,
        long newerThanTimestamp,
        long newerThanTimestampVersion,
        ReaderTx tx,
        boolean hydrateValues) throws Exception {

        ReadIndex memoryIndexReader = null;
        ReadIndex flushingMemoryIndexReader = null;
        try {
            while (true) {
                RawMemoryIndex memoryIndexStackCopy;
                RawMemoryIndex flushingMemoryIndexStackCopy;

                if (acquireCommitSemaphore) {
                    commitSemaphore.acquire();
                }
                try {
                    memoryIndexStackCopy = memoryIndex;
                    flushingMemoryIndexStackCopy = flushingMemoryIndex;
                } finally {
                    if (acquireCommitSemaphore) {
                        commitSemaphore.release();
                    }
                }

                if (mightContain(memoryIndexStackCopy, newerThanTimestamp, newerThanTimestampVersion)) {

                    memoryIndexReader = memoryIndexStackCopy.acquireReader();
                    if (memoryIndexReader != null) {
                        if (flushingMemoryIndexStackCopy != null) {
                            if (mightContain(flushingMemoryIndexStackCopy, newerThanTimestamp, newerThanTimestampVersion)) {
                                flushingMemoryIndexReader = flushingMemoryIndexStackCopy.acquireReader();
                                if (flushingMemoryIndexReader != null) {
                                    break;
                                } else {
                                    memoryIndexReader.release();
                                    memoryIndexReader = null;
                                }
                            }
                        } else {
                            break;
                        }
                    }
                } else if (flushingMemoryIndexStackCopy != null) {
                    if (mightContain(flushingMemoryIndexStackCopy, newerThanTimestamp, newerThanTimestampVersion)) {
                        flushingMemoryIndexReader = flushingMemoryIndexStackCopy.acquireReader();
                        if (flushingMemoryIndexReader != null) {
                            break;
                        }
                    }
                } else {
                    break;
                }

            }

            ReadIndex reader = memoryIndexReader;
            ReadIndex flushingReader = flushingMemoryIndexReader;
            return rangeStripedCompactableIndexes.rangeTx(index,
                from,
                to,
                newerThanTimestamp,
                newerThanTimestampVersion,
                (index1, fromKey, toKey, acquired, hydrateValues1) -> {
                    int active = (reader == null) ? 0 : 1;
                    int flushing = (flushingReader == null) ? 0 : 1;
                    ReadIndex[] indexes = new ReadIndex[acquired.length + active + flushing];
                    int i = 0;
                    if (reader != null) {
                        indexes[i] = reader;
                        i++;
                    }
                    if (flushingReader != null) {
                        indexes[i] = flushingReader;
                        i++;
                    }
                    System.arraycopy(acquired, 0, indexes, active + flushing, acquired.length);
                    pointTxCalled.incrementAndGet();
                    pointTxIndexCount.addAndGet(indexes.length);
                    return tx.tx(index1, fromKey, toKey, indexes, hydrateValues1);
                },
                hydrateValues
            );
        } finally {
            if (memoryIndexReader != null) {
                memoryIndexReader.release();
            }
            if (flushingMemoryIndexReader != null) {
                flushingMemoryIndexReader.release();
            }
        }

    }

    private boolean mightContain(RawMemoryIndex memoryIndexStackCopy, long newerThanTimestamp, long newerThanTimestampVersion) {
        TimestampAndVersion timestampAndVersion = memoryIndexStackCopy.maxTimestampAndVersion();
        return rawhide.mightContain(timestampAndVersion.maxTimestamp,
            timestampAndVersion.maxTimestampVersion,
            newerThanTimestamp,
            newerThanTimestampVersion);
    }

    private boolean isNewerThan(long timestamp, long timestampVersion, RawMemoryIndex memoryIndexStackCopy) {
        TimestampAndVersion timestampAndVersion = memoryIndexStackCopy.maxTimestampAndVersion();
        return rawhide.isNewerThan(timestamp,
            timestampVersion,
            timestampAndVersion.maxTimestamp,
            timestampAndVersion.maxTimestampVersion);
    }

    @Override
    public boolean journaledAppend(Values values, boolean fsyncAfterAppend) throws Exception {
        return internalAppend(true, fsyncAfterAppend, values, fsyncAfterAppend);
    }

    @Override
    public boolean append(Values values, boolean fsyncOnFlush) throws Exception {
        return internalAppend(false, false, values, fsyncOnFlush);
    }

    private boolean internalAppend(boolean appendToWal,
        boolean fsyncAfterAppend,
        Values values,
        boolean fsyncOnFlush) throws Exception, InterruptedException {
        if (values == null) {
            return false;
        }

        boolean appended;
        commitSemaphore.acquire();
        try {
            if (closeRequested.get()) {
                throw new LABIndexClosedException();
            }

            long appendVersion;
            if (appendToWal) {
                appendVersion = walAppendVersion.incrementAndGet();
            } else {
                appendVersion = -1;
            }

            appended = memoryIndex.append(
                (stream) -> {
                    RawEntryFormat format = rawEntryFormat.get();
                    return values.consume(
                        (index, key, timestamp, tombstoned, version, value) -> {

                            byte[] rawEntry = rawhide.toRawEntry(key, timestamp, tombstoned, version, value);
                            if (appendToWal) {
                                wal.append(walId, appendVersion, rawEntry);
                            }

                            RawMemoryIndex copy = flushingMemoryIndex;
                            TimestampAndVersion timestampAndVersion = rangeStripedCompactableIndexes.maxTimeStampAndVersion();
                            if ((copy == null || isNewerThan(timestamp, version, copy))
                            && rawhide.isNewerThan(timestamp, version, timestampAndVersion.maxTimestamp, timestampAndVersion.maxTimestampVersion)) {
                                return stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, rawEntry, 0, rawEntry.length);
                            } else {
                                rangeTx(false, -1, key, key, timestamp, version,
                                    (index1, fromKey, toKey, readIndexes, hydrateValues1) -> {
                                        GetRaw getRaw = IndexUtil.get(readIndexes);
                                        return getRaw.get(key,
                                            (existingReadKeyFormatTransformer, existingReadValueFormatTransformer, existingEntry, offset, length) -> {
                                                if (existingEntry == null) {
                                                    return stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, rawEntry, 0, rawEntry.length);
                                                } else {
                                                    long existingTimestamp = rawhide.timestamp(existingReadKeyFormatTransformer,
                                                        existingReadValueFormatTransformer,
                                                        existingEntry, offset, length);

                                                    long existingVersion = rawhide.version(existingReadKeyFormatTransformer,
                                                        existingReadValueFormatTransformer,
                                                        existingEntry, offset, length);

                                                    if (rawhide.isNewerThan(timestamp, version, existingTimestamp, existingVersion)) {
                                                        return stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, rawEntry, 0, rawEntry.length);
                                                    }
                                                }
                                                return false;
                                            }
                                        );
                                    },
                                    false // This prevent values from being hydrated
                                );
                                return true;
                            }
                        }
                    );
                }
            );

            if (appended && appendToWal) {
                wal.flush(walId, appendVersion, fsyncOnFlush);
            }

        } finally {
            commitSemaphore.release();
        }

        labHeapFlusher.commitIfNecessary(this, maxHeapPressureInBytes, fsyncOnFlush);
        return appended;
    }

    @Override
    public List<Future<Object>> commit(boolean fsync, boolean waitIfToFarBehind) throws Exception {
        if (memoryIndex.isEmpty()) {
            return Collections.emptyList();
        }

        if (corrupt) {
            throw new LABIndexCorruptedException();
        }
        if (closeRequested.get()) {
            throw new LABIndexClosedException();
        }

        if (!internalCommit(fsync)) {
            return Collections.emptyList();
        }
        if (waitIfToFarBehind) {
            return compact(fsync, minDebt, maxDebt, waitIfToFarBehind);
        } else {
            return Collections.singletonList(schedule.submit(() -> compact(fsync, minDebt, maxDebt, waitIfToFarBehind)));
        }
    }

    private boolean internalCommit(boolean fsync) throws Exception, InterruptedException {
        commitSemaphore.acquire(Short.MAX_VALUE);
        try {
            RawMemoryIndex stackCopy = memoryIndex;
            if (stackCopy.isEmpty()) {
                return false;
            }
            flushingMemoryIndex = stackCopy;
            memoryIndex = new RawMemoryIndex(destroy, labHeapFlusher.globalHeapCostInBytes(), rawhide);
            rangeStripedCompactableIndexes.append(stackCopy, fsync);
            flushingMemoryIndex = null;
            stackCopy.destroy();
        } finally {
            commitSemaphore.release(Short.MAX_VALUE);
        }
        return true;
    }

    @Override
    public List<Future<Object>> compact(boolean fsync, int minDebt, int maxDebt, boolean waitIfToFarBehind) throws Exception {

        int debt = rangeStripedCompactableIndexes.debt();
        if (debt == 0 || debt < minDebt) {
            return Collections.emptyList();
        }

        List<Future<Object>> awaitable = null;
        while (!closeRequested.get()) {
            if (corrupt) {
                throw new LABIndexCorruptedException();
            }
            List<Callable<Void>> compactors = rangeStripedCompactableIndexes.buildCompactors(fsync, minDebt);
            if (compactors != null && !compactors.isEmpty()) {
                if (awaitable == null) {
                    awaitable = new ArrayList<>(compactors.size());
                }
                for (Callable<Void> compactor : compactors) {
                    LOG.debug("Scheduling async compaction:{} for index:{} debt:{}", compactors, rangeStripedCompactableIndexes, debt);
                    synchronized (compactLock) {
                        if (closeRequested.get()) {
                            break;
                        } else {
                            ongoingCompactions.incrementAndGet();
                        }
                    }
                    Future<Object> future = compact.submit(() -> {
                        try {
                            compactor.call();
                        } catch (Exception x) {
                            LOG.error("Failed to compact " + rangeStripedCompactableIndexes, x);
                            corrupt = true;
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

            if (waitIfToFarBehind && debt >= maxDebt) {
                synchronized (compactLock) {
                    if (!closeRequested.get() && ongoingCompactions.get() > 0) {
                        LOG.debug("Waiting because debt is too high for index:{} debt:{}", rangeStripedCompactableIndexes, debt);
                        compactLock.wait();
                    } else {
                        break;
                    }
                }
                debt = rangeStripedCompactableIndexes.debt();
            } else {
                break;
            }
        }
        return awaitable;
    }

    @Override
    public void close(boolean flushUncommited, boolean fsync) throws Exception {
        labHeapFlusher.close(this);

        if (!closeRequested.compareAndSet(false, true)) {
            throw new LABIndexClosedException();
        }

        if (flushUncommited) {
            internalCommit(fsync);
        }

        synchronized (compactLock) {
            while (ongoingCompactions.get() > 0) {
                compactLock.wait();
            }
        }

        commitSemaphore.acquire(Short.MAX_VALUE);
        try {
            memoryIndex.closeReadable();
            rangeStripedCompactableIndexes.close();
            memoryIndex.destroy();
        } finally {
            commitSemaphore.release(Short.MAX_VALUE);
        }
        LOG.debug("Closed {}", this);

    }

    @Override
    public String toString() {
        return "LAB{"
            + ", minDebt=" + minDebt
            + ", maxDebt=" + maxDebt
            + ", ongoingCompactions=" + ongoingCompactions
            + ", corrupt=" + corrupt
            + ", rawhide=" + rawhide
            + '}';
    }

    private boolean rawToReal(int index, byte[] key, GetRaw getRaw, ValueStream valueStream, boolean hydrateValues) throws Exception {
        return getRaw.get(key,
            (readKeyFormatTransformer, readValueFormatTransformer, rawEntry, offset, length) -> {
                return rawhide.streamRawEntry(index, readKeyFormatTransformer, readValueFormatTransformer, rawEntry, offset, valueStream, hydrateValues);
            }
        );
    }

    private boolean rawToReal(int index, NextRawEntry nextRawEntry, ValueStream valueStream, boolean hydrateValues) throws Exception {
        while (true) {
            Next next = nextRawEntry.next((readKeyFormatTransformer, readValueFormatTransformer, rawEntry, offset, length) -> {
                return rawhide.streamRawEntry(index, readKeyFormatTransformer, readValueFormatTransformer, rawEntry, offset, valueStream, hydrateValues);
            });
            if (next == Next.stopped) {
                return false;
            } else if (next == Next.eos) {
                return true;
            }
        }
    }

    public void auditRanges(KeyToString keyToString) {
        rangeStripedCompactableIndexes.auditRanges(keyToString);
    }

}
