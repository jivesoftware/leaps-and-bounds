package com.jivesoftware.os.lab;

import com.jivesoftware.os.lab.api.LABIndexCorruptedException;
import com.jivesoftware.os.lab.api.RawEntryMarshaller;
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
 *
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

    private volatile RawMemoryIndex memoryIndex;
    private volatile RawMemoryIndex flushingMemoryIndex;
    private volatile boolean corrupt = false;

    private final RawEntryMarshaller valueMerger;

    public LAB(RawEntryMarshaller valueMerger,
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

        this.valueMerger = valueMerger;
        this.compact = compact;
        this.destroy = destroy;
        this.maxUpdatesBeforeFlush = maxUpdatesBeforeFlush;
        this.memoryIndex = new RawMemoryIndex(destroy, valueMerger);
        this.rangeStripedCompactableIndexes = new RangeStripedCompactableIndexes(destroy,
            root,
            indexName,
            useMemMap,
            entriesBetweenLeaps,
            splitWhenKeysTotalExceedsNBytes,
            splitWhenValuesTotalExceedsNBytes,
            splitWhenValuesAndKeysTotalExceedsNBytes,
            valueMerger,
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
            return rawToReal(IndexUtil.rangeScan(readIndexes, from, to), stream);
        });
    }

    @Override
    public boolean rowScan(ValueStream stream) throws Exception {
        return tx(null, null, -1, -1, (readIndexes) -> {
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
        return valueMerger.mightContain(timestampAndVersion.maxTimestamp,
            timestampAndVersion.maxTimestampVersion,
            newerThanTimestamp,
            newerThanTimestampVersion);
    }

    private boolean isNewerThan(long timestamp, long timestampVersion, RawMemoryIndex memoryIndexStackCopy) {
        TimestampAndVersion timestampAndVersion = memoryIndexStackCopy.maxTimestampAndVersion();
        return valueMerger.isNewerThan(timestamp,
            timestampVersion,
            timestampAndVersion.maxTimestamp,
            timestampAndVersion.maxTimestampVersion);
    }

    @Override
    public boolean append(Values values) throws Exception {
        boolean appended;
        commitSemaphore.acquire();
        try {
            appended = memoryIndex.append((stream) -> {
                return values != null && values.consume((key, timestamp, tombstoned, version, value) -> {
                    byte[] rawEntry = valueMerger.toRawEntry(key, timestamp, tombstoned, version, value);

                    RawMemoryIndex copy = flushingMemoryIndex;
                    TimestampAndVersion timestampAndVersion = rangeStripedCompactableIndexes.maxTimeStampAndVersion();
                    if ((copy == null || isNewerThan(timestamp, version, copy))
                        && valueMerger.isNewerThan(timestamp, version, timestampAndVersion.maxTimestamp, timestampAndVersion.maxTimestampVersion)) {
                        return stream.stream(rawEntry, 0, rawEntry.length);
                    } else {
                        tx(key, key, timestamp, version, (readIndexes) -> {
                            GetRaw getRaw = IndexUtil.get(readIndexes);
                            return getRaw.get(key, (existingEntry, offset, length) -> {
                                if (existingEntry == null) {
                                    return stream.stream(rawEntry, 0, rawEntry.length);
                                } else {
                                    long existingTimestamp = valueMerger.timestamp(existingEntry, offset, length);
                                    long existingVersion = valueMerger.version(existingEntry, offset, length);
                                    if (valueMerger.isNewerThan(timestamp, version, existingTimestamp, existingVersion)) {
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
            commit(true);
        }
        return appended;
    }

    @Override
    public List<Future<Object>> commit(boolean fsync) throws Exception {

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
            memoryIndex = new RawMemoryIndex(destroy, valueMerger);
            rangeStripedCompactableIndexes.append(stackCopy, fsync);
            flushingMemoryIndex = null;
            stackCopy.destroy();
        } finally {
            commitSemaphore.release(numPermits);
        }

        return compact(fsync);
        //return Collections.emptyList();
    }

    public List<Future<Object>> compact(boolean fsync) throws Exception {

        int debt = rangeStripedCompactableIndexes.debt(minDebt);
        if (debt == 0) {
            return Collections.emptyList();
        }

        List<Future<Object>> awaitable = null;
        while (true) {
            if (corrupt) {
                throw new LABIndexCorruptedException();
            }
            List<Callable<Void>> compactors = rangeStripedCompactableIndexes.buildCompactors(minDebt, fsync);
            if (compactors != null && !compactors.isEmpty()) {

//                Set<Long> pre = new HashSet<>();
//                rowScan(new ValueStream() {
//                    @Override
//                    public boolean stream(byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) throws Exception {
//                        pre.add(UIO.bytesLong(key));
//                        return true;
//                    }
//                });
//                System.out.println(" >> " + pre.size() + " Premerge:" + pre);
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

//                for (Future<Object> future : awaitable) {
//                    future.get();
//                }
//                Set<Long> post = new HashSet<>();
//                rowScan(new ValueStream() {
//                    @Override
//                    public boolean stream(byte[] key, long timestamp, boolean tombstoned, long version, byte[] payload) throws Exception {
//                        if (payload != null) {
//                            post.add(UIO.bytesLong(key));
//                        }
//                        return true;
//                    }
//                });
//                System.out.println(" << " + post.size() + " PostMerge:" + post);
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
            + ", corrupt=" + corrupt
            + ", valueMerger=" + valueMerger
            + '}';
    }

    private boolean rawToReal(byte[] key, GetRaw getRaw, ValueStream valueStream) throws Exception {
        return getRaw.get(key, (rawEntry, offset, length) -> valueMerger.streamRawEntry(valueStream, rawEntry, offset));
    }

    private boolean rawToReal(NextRawEntry nextRawEntry, ValueStream valueStream) throws Exception {
        while (true) {
            Next next = nextRawEntry.next((rawEntry, offset, length) -> {
                return valueMerger.streamRawEntry(valueStream, rawEntry, offset);
            });
            if (next == Next.stopped) {
                return false;
            } else if (next == Next.eos) {
                return true;
            }
        }
    }

}
