package com.jivesoftware.os.lab;

import com.jivesoftware.os.lab.api.LABIndexCorruptedException;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.api.ValueIndex;
import com.jivesoftware.os.lab.api.ValueStream;
import com.jivesoftware.os.lab.api.Values;
import com.jivesoftware.os.lab.guts.IndexUtil;
import com.jivesoftware.os.lab.guts.RangeStripedCompactableIndexes;
import com.jivesoftware.os.lab.guts.RawMemoryIndex;
import com.jivesoftware.os.lab.guts.ReaderTx;
import com.jivesoftware.os.lab.guts.TimestampAndVersion;
import com.jivesoftware.os.lab.guts.api.GetRaw;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jonathan.colt
 */
public class LAB implements ValueIndex {

    static private class CompactLock {
    }

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();
    private static final int numPermits = 1024;

    private final ExecutorService destroy;
    private final ExecutorService compact;
    private final int maxUpdatesBeforeFlush;
    private final int minDebt;
    private final int maxDebt;
    private final RangeStripedCompactableIndexes rangeStripedCompactableIndexes;
    private final Semaphore commitSemaphore = new Semaphore(numPermits, true); // TODO expose?
    private final CompactLock compactLock = new CompactLock();
    private final AtomicLong ongoingCompactions = new AtomicLong();
    private volatile boolean closeRequested = false;

    private volatile RawMemoryIndex memoryIndex;
    private volatile RawMemoryIndex flushingMemoryIndex;
    private volatile boolean corrupt = false;

    private final Rawhide rawhide;

    public LAB(Rawhide rawhide,
        ExecutorService compact,
        ExecutorService destroy,
        File root,
        String indexName,
        boolean useMemMap,
        int entriesBetweenLeaps,
        int maxUpdatesBeforeFlush,
        int minDebt,
        int maxDebt,
        long splitWhenKeysTotalExceedsNBytes,
        long splitWhenValuesTotalExceedsNBytes,
        long splitWhenValuesAndKeysTotalExceedsNBytes,
        int concurrency) throws Exception {

        this.rawhide = rawhide;
        this.compact = compact;
        this.destroy = destroy;
        this.maxUpdatesBeforeFlush = maxUpdatesBeforeFlush;
        this.memoryIndex = new RawMemoryIndex(destroy, rawhide);
        this.rangeStripedCompactableIndexes = new RangeStripedCompactableIndexes(destroy,
            root,
            indexName,
            useMemMap,
            entriesBetweenLeaps,
            splitWhenKeysTotalExceedsNBytes,
            splitWhenValuesTotalExceedsNBytes,
            splitWhenValuesAndKeysTotalExceedsNBytes,
            rawhide,
            concurrency);
        this.minDebt = minDebt;
        this.maxDebt = maxDebt;
    }

    @Override
    public int debt() throws Exception {
        return rangeStripedCompactableIndexes.debt(minDebt);
    }

    // TODO multi get keys
    @Override
    public boolean get(byte[] key, ValueStream stream) throws Exception {
        return tx(key, key, -1, -1, (readIndexes) -> {
            GetRaw getRaw = IndexUtil.get(readIndexes);
            return rawToReal(key, getRaw, stream);
        });
    }

    @Override
    public boolean rangeScan(byte[] from, byte[] to, ValueStream stream) throws Exception {
        return tx(from, to, -1, -1, (readIndexes) -> {
            return rawToReal(IndexUtil.rangeScan(readIndexes, from, to, rawhide), stream);
        });
    }

    @Override
    public boolean rowScan(ValueStream stream) throws Exception {
        return tx(null, null, -1, -1, (readIndexes) -> {
            return rawToReal(IndexUtil.rowScan(readIndexes, rawhide), stream);
        });
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

    private boolean tx(byte[] from,
        byte[] to,
        long newerThanTimestamp,
        long newerThanTimestampVersion,
        ReaderTx tx) throws Exception {

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
            return rangeStripedCompactableIndexes.tx(from, to, newerThanTimestamp, newerThanTimestampVersion, acquired -> {

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
    public boolean append(Values values, boolean fsyncOnFlush) throws Exception {
        boolean appended;
        commitSemaphore.acquire();
        try {
            appended = memoryIndex.append((stream) -> {
                return values != null && values.consume((key, timestamp, tombstoned, version, value) -> {
                    byte[] rawEntry = rawhide.toRawEntry(key, timestamp, tombstoned, version, value);

                    RawMemoryIndex copy = flushingMemoryIndex;
                    TimestampAndVersion timestampAndVersion = rangeStripedCompactableIndexes.maxTimeStampAndVersion();
                    if ((copy == null || isNewerThan(timestamp, version, copy))
                        && rawhide.isNewerThan(timestamp, version, timestampAndVersion.maxTimestamp, timestampAndVersion.maxTimestampVersion)) {
                        return stream.stream(rawEntry, 0, rawEntry.length);
                    } else {
                        tx(key, key, timestamp, version, (readIndexes) -> {
                            GetRaw getRaw = IndexUtil.get(readIndexes);
                            return getRaw.get(key, (existingEntry, offset, length) -> {
                                if (existingEntry == null) {
                                    return stream.stream(rawEntry, 0, rawEntry.length);
                                } else {
                                    long existingTimestamp = rawhide.timestamp(existingEntry, offset, length);
                                    long existingVersion = rawhide.version(existingEntry, offset, length);
                                    if (rawhide.isNewerThan(timestamp, version, existingTimestamp, existingVersion)) {
                                        return stream.stream(rawEntry, 0, rawEntry.length);
                                    }
                                }
                                return false;
                            });
                        });
                        return true;
                    }
                });
            });

        } finally {
            commitSemaphore.release();
        }

        if (memoryIndex.count() > maxUpdatesBeforeFlush) {
            commit(fsyncOnFlush);
        }
        return appended;
    }

    @Override
    public List<Future<Object>> commit(boolean fsync) throws Exception {
        if (memoryIndex.isEmpty()) {
            return Collections.emptyList();
        }

        if (corrupt) {
            throw new LABIndexCorruptedException();
        }
        commitSemaphore.acquire(numPermits);
        try {
            RawMemoryIndex stackCopy = memoryIndex;
            if (stackCopy.isEmpty()) {
                return Collections.emptyList();
            }
            flushingMemoryIndex = stackCopy;
            memoryIndex = new RawMemoryIndex(destroy, rawhide);
            rangeStripedCompactableIndexes.append(stackCopy, fsync);
            flushingMemoryIndex = null;
            stackCopy.destroy();
        } finally {
            commitSemaphore.release(numPermits);
        }

        return compact(fsync);
    }

    private List<Future<Object>> compact(boolean fsync) throws Exception {

        int debt = rangeStripedCompactableIndexes.debt(minDebt);
        if (debt == 0) {
            return Collections.emptyList();
        }

        List<Future<Object>> awaitable = null;
        while (!closeRequested) {
            if (corrupt) {
                throw new LABIndexCorruptedException();
            }
            List<Callable<Void>> compactors = rangeStripedCompactableIndexes.buildCompactors(minDebt, fsync);
            if (compactors != null && !compactors.isEmpty()) {
                if (awaitable == null) {
                    awaitable = new ArrayList<>(compactors.size());
                }
                for (Callable<Void> compactor : compactors) {
                    LOG.info("Scheduling async compaction:{} for index:{} debt:{}", compactors, rangeStripedCompactableIndexes, debt);
                    synchronized (compactLock) {
                        if (closeRequested) {
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

            if (debt >= maxDebt) {
                synchronized (compactLock) {
                    if (!closeRequested && ongoingCompactions.get() > 0) {
                        LOG.debug("Waiting because debt is too high for index:{} debt:{}", rangeStripedCompactableIndexes, debt);
                        compactLock.wait();
                    } else {
                        break;
                    }
                }
                debt = rangeStripedCompactableIndexes.debt(minDebt);
            } else {
                break;
            }
        }
        return awaitable;
    }

    @Override
    public void close() throws Exception {

        closeRequested = true;
        synchronized (compactLock) {
            while (ongoingCompactions.get() > 0) {
                compactLock.wait();
            }
        }

        memoryIndex.closeReadable();
        rangeStripedCompactableIndexes.close();
    }

    @Override
    public String toString() {
        return "LAB{"
            + "maxUpdatesBeforeFlush=" + maxUpdatesBeforeFlush
            + ", minDebt=" + minDebt
            + ", maxDebt=" + maxDebt
            + ", ongoingCompactions=" + ongoingCompactions
            + ", corrupt=" + corrupt
            + ", rawhide=" + rawhide
            + '}';
    }

    private boolean rawToReal(byte[] key, GetRaw getRaw, ValueStream valueStream) throws Exception {
        return getRaw.get(key, (rawEntry, offset, length) -> rawhide.streamRawEntry(valueStream, rawEntry, offset));
    }

    private boolean rawToReal(NextRawEntry nextRawEntry, ValueStream valueStream) throws Exception {
        while (true) {
            Next next = nextRawEntry.next((rawEntry, offset, length) -> {
                return rawhide.streamRawEntry(valueStream, rawEntry, offset);
            });
            if (next == Next.stopped) {
                return false;
            } else if (next == Next.eos) {
                return true;
            }
        }
    }

}
