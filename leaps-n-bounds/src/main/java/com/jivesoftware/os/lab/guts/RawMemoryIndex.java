package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.BolBuffer;
import com.jivesoftware.os.lab.LABConcurrentSkipListMap;
import com.jivesoftware.os.lab.LabHeapPressure;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.guts.api.AppendEntries;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.RawAppendableIndex;
import com.jivesoftware.os.lab.guts.api.RawConcurrentReadableIndex;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.guts.api.Scanner;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jonathan.colt
 */
public class RawMemoryIndex implements RawAppendableIndex, RawConcurrentReadableIndex {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final LABIndex index;
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

    public RawMemoryIndex(ExecutorService destroy,
        LabHeapPressure labHeapPressure,
        Rawhide rawhide,
        LABIndex index) throws InterruptedException {

        this.destroy = destroy;
        this.labHeapPressure = labHeapPressure;
        this.rawhide = rawhide;
        this.index = index;
        this.reader = new ReadIndex() {

            @Override
            public GetRaw get() throws Exception {

                return new GetRaw() {

                    private boolean result;

                    @Override
                    public boolean get(byte[] key, RawEntryStream stream) throws Exception {
                        BolBuffer rawEntry = index.get(new BolBuffer(key), new BolBuffer()); // Grrr
                        if (rawEntry == null) {
                            return false;
                        } else {
                            result = stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, rawEntry.asByteBuffer());
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
            public Scanner rangeScan(byte[] from, byte[] to) throws Exception {
//                Iterator<Map.Entry<byte[], byte[]>> iterator = subMap(from, to).entrySet().iterator();
//                return (stream) -> {
//                    if (iterator.hasNext()) {
//                        Map.Entry<byte[], byte[]> next = iterator.next();
//                        boolean more = stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, ByteBuffer.wrap(next.getValue()));
//                        return more ? Next.more : Next.stopped;
//                    }
//                    return Next.eos;
//                };
                return index.scanner(from, to);
            }

            @Override
            public Scanner rowScan() throws Exception {
//                Iterator<Map.Entry<byte[], byte[]>> iterator = index.entrySet().iterator();
//                return (stream) -> {
//                    if (iterator.hasNext()) {
//                        Map.Entry<byte[], byte[]> next = iterator.next();
//                        boolean more = stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, ByteBuffer.wrap(next.getValue()));
//                        return more ? Next.more : Next.stopped;
//                    }
//                    return Next.eos;
//                };
                return index.scanner(null, null);
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
    public boolean containsKeyInRange(byte[] from, byte[] to) throws Exception {
        return index.contains(from, to);
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
    public boolean append(AppendEntries entries, BolBuffer keyBuffer) throws Exception {
        BolBuffer valueBuffer = new BolBuffer(); // Grrrr
        return entries.consume((readKeyFormatTransformer, readValueFormatTransformer, rawEntryBuffer) -> {

            BolBuffer key = rawhide.key(readKeyFormatTransformer, readValueFormatTransformer, rawEntryBuffer, keyBuffer);
            int keyLength = key.length;
            index.compute(key, valueBuffer,
                (value) -> {

                    BolBuffer merged;
                    int valueLength = ((rawEntryBuffer.length == -1) ? 0 : rawEntryBuffer.length);
                    if (value == null) {
                        approximateCount.incrementAndGet();
                        keysCostInBytes.addAndGet(keyLength);
                        valuesCostInBytes.addAndGet(valueLength);
                        labHeapPressure.change(keyLength + valueLength);
                        merged = rawEntryBuffer;
                    } else {
                        merged = rawhide.merge(FormatTransformer.NO_OP, FormatTransformer.NO_OP, value,
                            readKeyFormatTransformer, readValueFormatTransformer,
                            rawEntryBuffer, FormatTransformer.NO_OP, FormatTransformer.NO_OP);

                        int mergeValueLength = ((merged.length == -1) ? 0 : merged.length);
                        int valueLengthDelta = mergeValueLength - valueLength;
                        valuesCostInBytes.addAndGet(valueLengthDelta);
                        labHeapPressure.change(valueLengthDelta);
                    }
                    long timestamp = rawhide.timestamp(FormatTransformer.NO_OP, FormatTransformer.NO_OP, merged);
                    long version = rawhide.version(FormatTransformer.NO_OP, FormatTransformer.NO_OP, merged);
                    updateMaxTimestampAndVersion(timestamp, version);
                    return merged;
                }
            );
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
            } catch (Throwable t) {
                LOG.error("Destroy failed horribly!", t);
                throw t;
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
    public boolean isEmpty() throws Exception {
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
    public byte[] minKey() throws Exception {
        return index.firstKey();
    }

    @Override
    public byte[] maxKey() throws Exception {
        return index.lastKey();
    }

    @Override
    public TimestampAndVersion maxTimestampAndVersion() {
        return maxTimestampAndVersion.get();
    }

}
