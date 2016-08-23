package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.LABConcurrentSkipListMap;
import com.jivesoftware.os.lab.LABIndexableMemory;
import com.jivesoftware.os.lab.LabHeapPressure;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.guts.api.AppendEntries;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.NextRawEntry;
import com.jivesoftware.os.lab.guts.api.NextRawEntry.Next;
import com.jivesoftware.os.lab.guts.api.RawAppendableIndex;
import com.jivesoftware.os.lab.guts.api.RawConcurrentReadableIndex;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jonathan.colt
 */
public class RawMemoryIndex implements RawAppendableIndex, RawConcurrentReadableIndex {

    private final SortedMap<byte[], byte[]> index;
    private final AtomicLong approximateCount = new AtomicLong();
    private final AtomicBoolean disposed = new AtomicBoolean(false);

    private final ExecutorService destroy;
    private final LabHeapPressure labHeapPressure;
    private final AtomicLong keysCostInBytes = new AtomicLong();
    private final AtomicLong valuesCostInBytes = new AtomicLong();

    private final Rawhide rawhide;

    private final int numBones = Short.MAX_VALUE;
    private final Semaphore hideABone = new Semaphore(numBones, true);
    private final ReadIndex reader;
    private AtomicReference<TimestampAndVersion> maxTimestampAndVersion = new AtomicReference<>(TimestampAndVersion.NULL);

    public RawMemoryIndex(ExecutorService destroy, LabHeapPressure labHeapPressure, Rawhide rawhide, boolean useOffHeap) {
        this.destroy = destroy;
        this.labHeapPressure = labHeapPressure;
        this.rawhide = rawhide;
        if (useOffHeap) {
            this.index = new LABConcurrentSkipListMap(new LABIndexableMemory(rawhide));
        } else {
            this.index = new ConcurrentSkipListMap<>(rawhide.getKeyComparator());
        }
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
                            result = stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, ByteBuffer.wrap(rawEntry));
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
                        boolean more = stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, ByteBuffer.wrap(next.getValue()));
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
                        boolean more = stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, ByteBuffer.wrap(next.getValue()));
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

    @Override
    public boolean containsKeyInRange(byte[] from, byte[] to) {
        return !subMap(from, to).isEmpty();
    }

    private Map<byte[], byte[]> subMap(byte[] from, byte[] to) {
        if (from != null && to != null) {
            return index.subMap(from, to);
        } else if (from != null) {
            return index.tailMap(from);
        } else if (to != null) {
            return index.headMap(to);
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
    public boolean append(AppendEntries entries) throws Exception {
        return entries.consume((readKeyFormatTransformer, readValueFormatTransformer, rawEntry, offset, length) -> {

            byte[] key = rawhide.key(readKeyFormatTransformer, readValueFormatTransformer, rawEntry, offset, length);
            int keyLength = key.length;
            index.compute(key, (k, value) -> {
                byte[] merged;
                int valueLength = ((rawEntry == null) ? 0 : rawEntry.length);
                if (value == null) {
                    approximateCount.incrementAndGet();
                    keysCostInBytes.addAndGet(keyLength);
                    valuesCostInBytes.addAndGet(valueLength);
                    labHeapPressure.change(keyLength + valueLength);
                    merged = rawEntry;
                } else {
                    merged = rawhide.merge(FormatTransformer.NO_OP, FormatTransformer.NO_OP, value,
                        readKeyFormatTransformer, readValueFormatTransformer,
                        rawEntry, FormatTransformer.NO_OP, FormatTransformer.NO_OP);

                    int mergeValueLength = ((merged == null) ? 0 : merged.length);
                    int valueLengthDelta = mergeValueLength - valueLength;
                    valuesCostInBytes.addAndGet(valueLengthDelta);
                    labHeapPressure.change(valueLengthDelta);
                }
                ByteBuffer bbMerged = ByteBuffer.wrap(merged);
                long timestamp = rawhide.timestamp(FormatTransformer.NO_OP, FormatTransformer.NO_OP, bbMerged);
                long version = rawhide.version(FormatTransformer.NO_OP, FormatTransformer.NO_OP, bbMerged);
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

                if (index instanceof LABConcurrentSkipListMap) {
                    ((LABConcurrentSkipListMap) index).freeAll();
                }
                index.clear();
                labHeapPressure.change(-sizeInBytes());
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
        return keysCostInBytes.get() + valuesCostInBytes.get();
    }

    @Override
    public long keysSizeInBytes() throws IOException {
        return keysCostInBytes.get();
    }

    @Override
    public long valuesSizeInBytes() throws IOException {
        return valuesCostInBytes.get();
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
