package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.NextRawEntry;
import com.jivesoftware.os.lab.guts.api.NextRawEntry.Next;
import com.jivesoftware.os.lab.guts.api.RawAppendableIndex;
import com.jivesoftware.os.lab.guts.api.RawConcurrentReadableIndex;
import com.jivesoftware.os.lab.guts.api.RawEntries;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jonathan.colt
 */
public class RawMemoryIndex implements RawAppendableIndex, RawConcurrentReadableIndex, RawEntries {

    private final ConcurrentSkipListMap<byte[], byte[]> index;
    private final AtomicLong approximateCount = new AtomicLong();
    private final AtomicBoolean disposed = new AtomicBoolean(false);

    private final ExecutorService destroy;
    private final AtomicLong globalHeapCostInBytes;
    private volatile long keysCostInBytes = 0;
    private volatile long valuesCostInBytes = 0;

    private final Rawhide rawhide;

    private final int numBones = Short.MAX_VALUE;
    private final Semaphore hideABone = new Semaphore(numBones, true);
    private final ReadIndex reader;
    private AtomicReference<TimestampAndVersion> maxTimestampAndVersion = new AtomicReference<>(TimestampAndVersion.NULL);

    public RawMemoryIndex(ExecutorService destroy, AtomicLong globalHeapCostInBytes, Rawhide rawhide) {
        this.destroy = destroy;
        this.globalHeapCostInBytes = globalHeapCostInBytes;
        this.rawhide = rawhide;
        this.index = new ConcurrentSkipListMap<>(rawhide);
        this.reader = new ReadIndex() {

            @Override
            public GetRaw get() throws Exception {

                return new GetRaw() {

                    private boolean result;

                    @Override
                    public boolean get(byte[] key, RawEntryStream stream) throws Exception {
                        byte[] rawEntry = index.get(key);
                        if (rawEntry == null) {
                            return false;
                        } else {
                            result = stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, rawEntry, 0, rawEntry.length);
                            return true;
                        }
                    }

                    @Override
                    public boolean result() {
                        return result;
                    }
                };
            }

            @Override
            public NextRawEntry rangeScan(byte[] from, byte[] to) throws Exception {
                Iterator<Map.Entry<byte[], byte[]>> iterator = subMap(from, to).entrySet().iterator();
                return (stream) -> {
                    if (iterator.hasNext()) {
                        Map.Entry<byte[], byte[]> next = iterator.next();
                        boolean more = stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, next.getValue(), 0, next.getValue().length);
                        return more ? Next.more : Next.stopped;
                    }
                    return Next.eos;
                };
            }

            @Override
            public NextRawEntry rowScan() throws Exception {
                Iterator<Map.Entry<byte[], byte[]>> iterator = index.entrySet().iterator();
                return (stream) -> {
                    if (iterator.hasNext()) {
                        Map.Entry<byte[], byte[]> next = iterator.next();
                        boolean more = stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, next.getValue(), 0, next.getValue().length);
                        return more ? Next.more : Next.stopped;
                    }
                    return Next.eos;
                };
            }

            @Override
            public void close() {
                throw new UnsupportedOperationException("NOPE");
            }

            @Override
            public long count() {
                throw new UnsupportedOperationException("NOPE");
            }

            @Override
            public boolean isEmpty() {
                throw new UnsupportedOperationException("NOPE");
            }

            @Override
            public void release() {
                hideABone.release();
            }
        };
    }

    public boolean containsKeyInRange(byte[] from, byte[] to) {
        return !subMap(from, to).isEmpty();
    }

    private Map<byte[], byte[]> subMap(byte[] from, byte[] to) {
        if (from != null && to != null) {
            return index.subMap(from, true, to, false);
        } else if (from != null) {
            return index.tailMap(from, true);
        } else if (to != null) {
            return index.headMap(to, false);
        } else {
            return index;
        }
    }

    @Override
    public String name() {
        return "RawMemoryIndex";
    }

    @Override
    public IndexRangeId id() {
        return null;
    }

    @Override
    public boolean consume(RawEntryStream stream) throws Exception {
        for (Map.Entry<byte[], byte[]> e : index.entrySet()) {
            byte[] entry = e.getValue();
            if (!stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, entry, 0, entry.length)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean append(RawEntries entries) throws Exception {
        return entries.consume((readKeyFormatTransformer, readValueFormatTransformer, rawEntry, offset, length) -> {

            byte[] key = rawhide.key(readKeyFormatTransformer, readValueFormatTransformer, rawEntry, offset, length);
            int keyLength = key.length;
            index.compute(key, (k, value) -> {
                byte[] merged;
                int valueLength = ((rawEntry == null) ? 0 : rawEntry.length);
                if (value == null) {
                    approximateCount.incrementAndGet();
                    keysCostInBytes += keyLength;
                    valuesCostInBytes += valueLength;
                    globalHeapCostInBytes.addAndGet(keyLength + valueLength);
                    merged = rawEntry;
                } else {
                    merged = rawhide.merge(FormatTransformer.NO_OP, FormatTransformer.NO_OP, value,
                        readKeyFormatTransformer, readValueFormatTransformer,
                        rawEntry, FormatTransformer.NO_OP, FormatTransformer.NO_OP);

                    int mergeValueLength = ((merged == null) ? 0 : merged.length);
                    int valueLengthDelta = mergeValueLength - valueLength;
                    valuesCostInBytes += valueLengthDelta;
                    globalHeapCostInBytes.addAndGet(valueLengthDelta);
                }
                long timestamp = rawhide.timestamp(FormatTransformer.NO_OP, FormatTransformer.NO_OP, merged, 0, merged.length);
                long version = rawhide.version(FormatTransformer.NO_OP, FormatTransformer.NO_OP, merged, 0, merged.length);
                updateMaxTimestampAndVersion(timestamp, version);
                return merged;
            });
            return true;
        });
    }

    private void updateMaxTimestampAndVersion(long timestamp, long version) {
        TimestampAndVersion existing = maxTimestampAndVersion.get();
        TimestampAndVersion update = new TimestampAndVersion(timestamp, version);
        while (rawhide.isNewerThan(timestamp, version, existing.maxTimestamp, existing.maxTimestampVersion)) {
            if (maxTimestampAndVersion.compareAndSet(existing, update)) {
                break;
            } else {
                existing = maxTimestampAndVersion.get();
            }
        }
    }

    // you must call release on this reader! Try to only use it as long as you have to!
    @Override
    public ReadIndex acquireReader() throws Exception {
        hideABone.acquire();
        if (disposed.get()) {
            hideABone.release();
            return null;
        }
        return reader;
    }

    @Override
    public void destroy() throws Exception {
        destroy.submit(() -> {
            hideABone.acquire(numBones);
            try {
                disposed.set(true);
                index.clear();
                globalHeapCostInBytes.addAndGet(-sizeInBytes());
            } finally {
                hideABone.release(numBones);
            }
            return null;
        });
    }

    @Override
    public void closeReadable() throws Exception {
    }

    @Override
    public void closeAppendable(boolean fsync) throws Exception {
    }

    @Override
    public boolean isEmpty() {
        return index.isEmpty();
    }

    @Override
    public long count() {
        return approximateCount.get();
    }

    @Override
    public long sizeInBytes() {
        return keysCostInBytes + valuesCostInBytes;
    }

    @Override
    public long keysSizeInBytes() throws IOException {
        return keysCostInBytes;
    }

    @Override
    public long valuesSizeInBytes() throws IOException {
        return valuesCostInBytes;
    }

    @Override
    public byte[] minKey() {
        return index.firstKey();
    }

    @Override
    public byte[] maxKey() {
        return index.lastKey();
    }

    @Override
    public TimestampAndVersion maxTimestampAndVersion() {
        return maxTimestampAndVersion.get();
    }

}
