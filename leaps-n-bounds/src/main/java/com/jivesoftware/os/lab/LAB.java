package com.jivesoftware.os.lab;

import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.api.AppendValues;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.FormatTransformerProvider;
import com.jivesoftware.os.lab.api.Keys;
import com.jivesoftware.os.lab.api.Ranges;
import com.jivesoftware.os.lab.api.RawEntryFormat;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.api.exceptions.LABClosedException;
import com.jivesoftware.os.lab.api.exceptions.LABCorruptedException;
import com.jivesoftware.os.lab.api.rawhide.Rawhide;
import com.jivesoftware.os.lab.guts.InterleaveStream;
import com.jivesoftware.os.lab.guts.LABHashIndexType;
import com.jivesoftware.os.lab.guts.LABIndex;
import com.jivesoftware.os.lab.guts.LABIndexProvider;
import com.jivesoftware.os.lab.guts.LABMemoryIndex;
import com.jivesoftware.os.lab.guts.Leaps;
import com.jivesoftware.os.lab.guts.PointInterleave;
import com.jivesoftware.os.lab.guts.RangeStripedCompactableIndexes;
import com.jivesoftware.os.lab.guts.ReaderTx;
import com.jivesoftware.os.lab.guts.api.KeyToString;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.guts.api.Scanner;
import com.jivesoftware.os.lab.guts.api.Scanner.Next;
import com.jivesoftware.os.lab.io.BolBuffer;
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
public class LAB implements ValueIndex<byte[]> {

    static private class CompactLock {
    }

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final static byte[] SMALLEST_POSSIBLE_KEY = new byte[0];

    private final ExecutorService schedule;
    private final ExecutorService compact;
    private final ExecutorService destroy;
    private final LabWAL wal;
    private final byte[] walId;
    private final AtomicLong walAppendVersion = new AtomicLong(0);

    private final LabHeapPressure labHeapPressure;
    private final long maxHeapPressureInBytes;
    private final int minDebt;
    private final int maxDebt;
    private final String primaryName;
    private final RangeStripedCompactableIndexes rangeStripedCompactableIndexes;
    private final Semaphore commitSemaphore = new Semaphore(Short.MAX_VALUE, true);
    private final CompactLock compactLock = new CompactLock();
    private final AtomicLong ongoingCompactions = new AtomicLong();
    private final AtomicBoolean closeRequested = new AtomicBoolean(false);

    private volatile LABMemoryIndex memoryIndex;
    private volatile LABMemoryIndex flushingMemoryIndex;
    private volatile boolean corrupt = false;

    private final LABStats stats;
    private final String rawhideName;
    private final Rawhide rawhide;
    private final LABIndexProvider<BolBuffer, BolBuffer> indexProvider;
    private final boolean hashIndexEnabled;

    private volatile long lastAppendTimestamp = 0;
    private volatile long lastCommitTimestamp = System.currentTimeMillis();

    public LAB(LABStats stats,
        FormatTransformerProvider formatTransformerProvider,
        String rawhideName,
        Rawhide rawhide,
        RawEntryFormat rawEntryFormat,
        ExecutorService schedule,
        ExecutorService compact,
        ExecutorService destroy,
        File root,
        LabWAL wal,
        byte[] walId,
        String primaryName,
        int entriesBetweenLeaps,
        LabHeapPressure labHeapPressure,
        long maxHeapPressureInBytes,
        int minDebt,
        int maxDebt,
        long splitWhenKeysTotalExceedsNBytes,
        long splitWhenValuesTotalExceedsNBytes,
        long splitWhenValuesAndKeysTotalExceedsNBytes,
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache,
        LABIndexProvider<BolBuffer, BolBuffer> indexProvider,
        boolean fsyncFileRenames,
        LABHashIndexType hashIndexType,
        double hashIndexLoadFactor,
        boolean hashIndexEnabled) throws Exception {

        stats.open.increment();

        this.stats = stats;
        this.rawhideName = rawhideName;
        this.rawhide = rawhide;
        this.schedule = schedule;
        this.compact = compact;
        this.destroy = destroy;
        this.wal = wal;
        this.walId = walId;
        this.labHeapPressure = labHeapPressure;
        this.maxHeapPressureInBytes = maxHeapPressureInBytes;
        this.memoryIndex = new LABMemoryIndex(destroy, labHeapPressure, stats, rawhide, indexProvider.create(rawhide, -1));
        this.primaryName = primaryName;
        this.rangeStripedCompactableIndexes = new RangeStripedCompactableIndexes(stats,
            destroy,
            root,
            primaryName,
            entriesBetweenLeaps,
            splitWhenKeysTotalExceedsNBytes,
            splitWhenValuesTotalExceedsNBytes,
            splitWhenValuesAndKeysTotalExceedsNBytes,
            formatTransformerProvider,
            rawhide,
            new AtomicReference<>(rawEntryFormat),
            leapsCache,
            fsyncFileRenames,
            hashIndexType,
            hashIndexLoadFactor);
        this.minDebt = minDebt;
        this.maxDebt = maxDebt;
        this.indexProvider = indexProvider;
        this.hashIndexEnabled = hashIndexEnabled;
    }

    @Override
    public String name() {
        return primaryName;
    }

    @Override
    public int debt() throws Exception {
        return rangeStripedCompactableIndexes.debt();
    }

    @Override
    public boolean get(Keys keys, ValueStream stream, boolean hydrateValues) throws Exception {
        BolBuffer streamKeyBuffer = new BolBuffer();
        BolBuffer streamValueBuffer = hydrateValues ? new BolBuffer() : null;

        boolean b = pointTx(keys,
            -1,
            -1,
            (index, fromKey, toKey, readIndexes, hydrateValues1) -> {
                PointInterleave pointInterleave = new PointInterleave(readIndexes, fromKey, rawhide, hashIndexEnabled);
                try {
                    return rawToReal(index, pointInterleave, streamKeyBuffer, streamValueBuffer, stream);
                } finally {
                    pointInterleave.close();
                }
            },
            hydrateValues
        );
        stats.gets.increment();
        return b;
    }

    @Override
    public boolean rangeScan(byte[] from, byte[] to, ValueStream stream, boolean hydrateValues) throws Exception {
        BolBuffer streamKeyBuffer = new BolBuffer();
        BolBuffer streamValueBuffer = hydrateValues ? new BolBuffer() : null;

        boolean r = rangeTx(true, -1, from, to, -1, -1,
            (index, fromKey, toKey, readIndexes, hydrateValues1) -> {
                InterleaveStream interleaveStream = new InterleaveStream(readIndexes, fromKey, toKey, rawhide);
                try {
                    return rawToReal(index, interleaveStream, streamKeyBuffer, streamValueBuffer, stream);
                } finally {
                    interleaveStream.close();
                }
            },
            hydrateValues
        );
        stats.rangeScan.increment();
        return r;
    }

    @Override
    public boolean rangesScan(Ranges ranges, ValueStream stream, boolean hydrateValues) throws Exception {
        BolBuffer streamKeyBuffer = new BolBuffer();
        BolBuffer streamValueBuffer = hydrateValues ? new BolBuffer() : null;
        boolean r = ranges.ranges((index, from, to) -> {
            return rangeTx(true, index, from, to, -1, -1,
                (index1, fromKey, toKey, readIndexes, hydrateValues1) -> {
                    InterleaveStream interleaveStream = new InterleaveStream(readIndexes, fromKey, toKey, rawhide);
                    try {
                        return rawToReal(index1, interleaveStream, streamKeyBuffer, streamValueBuffer, stream);
                    } finally {
                        interleaveStream.close();
                    }
                },
                hydrateValues
            );
        });
        stats.multiRangeScan.increment();
        return r;

    }

    @Override
    public boolean rowScan(ValueStream stream, boolean hydrateValues) throws Exception {
        BolBuffer streamKeyBuffer = new BolBuffer();
        BolBuffer streamValueBuffer = hydrateValues ? new BolBuffer() : null;
        boolean r = rangeTx(true, -1, SMALLEST_POSSIBLE_KEY, null, -1, -1,
            (index, fromKey, toKey, readIndexes, hydrateValues1) -> {
                InterleaveStream interleaveStream = new InterleaveStream(readIndexes, fromKey, toKey, rawhide);
                try {
                    return rawToReal(index, interleaveStream, streamKeyBuffer, streamValueBuffer, stream);
                } finally {
                    interleaveStream.close();
                }
            },
            hydrateValues
        );
        stats.rowScan.increment();
        return r;
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
        LABMemoryIndex stackCopyFlushingMemoryIndex = flushingMemoryIndex;
        LABMemoryIndex stackCopyMemoryIndex = memoryIndex;

        return memoryIndex.sizeInBytes()
            + ((stackCopyFlushingMemoryIndex != null && stackCopyFlushingMemoryIndex != stackCopyMemoryIndex) ? stackCopyFlushingMemoryIndex.sizeInBytes() : 0);
    }

    public long lastAppendTimestamp() {
        return lastAppendTimestamp;
    }

    public long lastCommitTimestamp() {
        return lastCommitTimestamp;
    }

    private boolean pointTx(Keys keys,
        long newerThanTimestamp,
        long newerThanTimestampVersion,
        ReaderTx tx,
        boolean hydrateValues) throws Exception {

        ReadIndex memoryIndexReader = null;
        ReadIndex flushingMemoryIndexReader = null;
        try {
            while (!closeRequested.get()) {
                LABMemoryIndex memoryIndexStackCopy;
                LABMemoryIndex flushingMemoryIndexStackCopy;

                commitSemaphore.acquire();
                try {
                    memoryIndexStackCopy = memoryIndex;
                    flushingMemoryIndexStackCopy = flushingMemoryIndex;
                } finally {
                    commitSemaphore.release();
                }

                if (memoryIndexStackCopy.mightContain(newerThanTimestamp, newerThanTimestampVersion)) {

                    memoryIndexReader = memoryIndexStackCopy.acquireReader();
                    if (memoryIndexReader != null) {
                        if (flushingMemoryIndexStackCopy != null) {
                            if (flushingMemoryIndexStackCopy.mightContain(newerThanTimestamp, newerThanTimestampVersion)) {
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
                    if (flushingMemoryIndexStackCopy.mightContain(newerThanTimestamp, newerThanTimestampVersion)) {
                        flushingMemoryIndexReader = flushingMemoryIndexStackCopy.acquireReader();
                        if (flushingMemoryIndexReader != null) {
                            break;
                        }
                    }
                } else {
                    break;
                }
            }

            if (closeRequested.get()) {
                throw new LABClosedException();
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
                LABMemoryIndex memoryIndexStackCopy;
                LABMemoryIndex flushingMemoryIndexStackCopy;

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

                if (memoryIndexStackCopy.mightContain(newerThanTimestamp, newerThanTimestampVersion)) {

                    memoryIndexReader = memoryIndexStackCopy.acquireReader();
                    if (memoryIndexReader != null) {
                        if (flushingMemoryIndexStackCopy != null) {
                            if (flushingMemoryIndexStackCopy.mightContain(newerThanTimestamp, newerThanTimestampVersion)) {
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
                    if (flushingMemoryIndexStackCopy.mightContain(newerThanTimestamp, newerThanTimestampVersion)) {
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

    @Override
    public boolean append(AppendValues<byte[]> values, boolean fsyncOnFlush,
        BolBuffer rawEntryBuffer, BolBuffer keyBuffer) throws Exception {
        return internalAppend(values, fsyncOnFlush, -1, rawEntryBuffer, keyBuffer, true);
    }

    public boolean onOpenAppend(AppendValues<byte[]> values, boolean fsyncOnFlush, long overrideMaxHeapPressureInBytes,
        BolBuffer rawEntryBuffer, BolBuffer keyBuffer) throws Exception {
        return internalAppend(values, fsyncOnFlush, overrideMaxHeapPressureInBytes, rawEntryBuffer, keyBuffer, false);
    }

    private boolean internalAppend(
        AppendValues<byte[]> values,
        boolean fsyncOnFlush,
        long overrideMaxHeapPressureInBytes,
        BolBuffer rawEntryBuffer,
        BolBuffer keyBuffer,
        boolean journal) throws Exception {

        if (values == null) {
            return false;
        }

        boolean[] appended = { false };
        commitSemaphore.acquire();
        try {
            if (closeRequested.get()) {
                throw new LABClosedException();
            }

            long appendVersion;
            if (journal && wal != null) {
                appendVersion = walAppendVersion.incrementAndGet();
            } else {
                appendVersion = -1;
            }
            lastAppendTimestamp = System.currentTimeMillis();

            long[] count = { 0 };
            if (journal && wal != null) {
                wal.appendTx(walId, appendVersion, fsyncOnFlush, activeWAL -> {
                    appended[0] = memoryIndex.append(
                        (stream) -> {
                            return values.consume(
                                (index, key, timestamp, tombstoned, version, value) -> {

                                    BolBuffer rawEntry = rawhide.toRawEntry(key, timestamp, tombstoned, version, value, rawEntryBuffer);
                                    activeWAL.append(walId, appendVersion, rawEntry);
                                    stats.journaledAppend.increment();

                                    count[0]++;
                                    return stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, rawEntry);
                                }
                            );
                        }, keyBuffer
                    );
                });
            } else {
                appended[0] = memoryIndex.append(
                    (stream) -> {
                        return values.consume(
                            (index, key, timestamp, tombstoned, version, value) -> {

                                BolBuffer rawEntry = rawhide.toRawEntry(key, timestamp, tombstoned, version, value, rawEntryBuffer);
                                stats.append.increment();
                                count[0]++;
                                return stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, rawEntry);
                            }
                        );
                    }, keyBuffer
                );
            }
            LOG.inc("append>count", count[0]);

        } finally {
            commitSemaphore.release();
        }
        labHeapPressure.commitIfNecessary(this, overrideMaxHeapPressureInBytes >= 0 ? overrideMaxHeapPressureInBytes : maxHeapPressureInBytes, fsyncOnFlush);
        return appended[0];
    }

    @Override
    public List<Future<Object>> commit(boolean fsync, boolean waitIfToFarBehind) throws Exception {
        if (memoryIndex.isEmpty()) {
            return Collections.emptyList();
        }

        if (corrupt) {
            throw new LABCorruptedException();
        }
        if (closeRequested.get()) {
            throw new LABClosedException();
        }

        if (!internalCommit(fsync, new BolBuffer(), new BolBuffer(), new BolBuffer())) { // grr
            return Collections.emptyList();
        }
        if (waitIfToFarBehind) {
            return compact(fsync, minDebt, maxDebt, true);
        } else {
            return Collections.singletonList(schedule.submit(() -> compact(fsync, minDebt, maxDebt, false)));
        }
    }

    private boolean internalCommit(boolean fsync, BolBuffer keyBuffer, BolBuffer entryBuffer, BolBuffer entryKeyBuffer) throws Exception {
        synchronized (commitSemaphore) {
            long appendVersion = -1;
            // open new memory index and mark existing for flush
            commitSemaphore.acquire(Short.MAX_VALUE);
            try {
                if (memoryIndex.isEmpty()) {
                    return false;
                }
                if (fsync) {
                    stats.fsyncedCommit.increment();
                } else {
                    stats.commit.increment();
                }
                if (wal != null) {
                    appendVersion = walAppendVersion.incrementAndGet();
                }
                flushingMemoryIndex = memoryIndex;
                LABIndex<BolBuffer, BolBuffer> labIndex = indexProvider.create(rawhide, flushingMemoryIndex.poweredUpTo());
                memoryIndex = new LABMemoryIndex(destroy, labHeapPressure, stats, rawhide, labIndex);
            } finally {
                commitSemaphore.release(Short.MAX_VALUE);
            }

            // flush existing memory index to disk
            rangeStripedCompactableIndexes.append(rawhideName, flushingMemoryIndex, fsync, keyBuffer, entryBuffer, entryKeyBuffer);

            // destroy existing memory index
            LABMemoryIndex destroyableMemoryIndex;
            commitSemaphore.acquire(Short.MAX_VALUE);
            try {
                destroyableMemoryIndex = flushingMemoryIndex;
                flushingMemoryIndex = null;
            } finally {
                commitSemaphore.release(Short.MAX_VALUE);
            }
            destroyableMemoryIndex.destroy();

            // commit to WAL
            if (wal != null) {
                wal.commit(walId, appendVersion, fsync);
            }
            lastCommitTimestamp = System.currentTimeMillis();

            return true;
        }
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
                throw new LABCorruptedException();
            }
            List<Callable<Void>> compactors = rangeStripedCompactableIndexes.buildCompactors(rawhideName, fsync, minDebt);
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
        labHeapPressure.close(this);

        if (!closeRequested.compareAndSet(false, true)) {
            throw new LABClosedException();
        }

        if (flushUncommited) {
            internalCommit(fsync, new BolBuffer(), new BolBuffer(), new BolBuffer()); // grr
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
        stats.closed.increment();

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

    private boolean rawToReal(int index,
        Scanner nextRawEntry,
        BolBuffer streamKeyBuffer,
        BolBuffer streamValueBuffer,
        ValueStream valueStream) throws Exception {
        while (true) {
            Next next = nextRawEntry.next((readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                return rawhide.streamRawEntry(index,
                    readKeyFormatTransformer,
                    readValueFormatTransformer,
                    rawEntry,
                    streamKeyBuffer,
                    streamValueBuffer,
                    valueStream);
            });
            if (next == Next.stopped) {
                return false;
            } else if (next == Next.eos) {
                return true;
            }
        }
    }

    public void auditRanges(KeyToString keyToString) throws Exception {
        rangeStripedCompactableIndexes.auditRanges(keyToString);
    }

}
