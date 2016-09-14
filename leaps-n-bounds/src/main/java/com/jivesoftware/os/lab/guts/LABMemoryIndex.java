package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.BolBuffer;
import com.jivesoftware.os.lab.LabHeapPressure;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.guts.LABIndex.Compute;
import com.jivesoftware.os.lab.guts.allocators.LABCostChangeInBytes;
import com.jivesoftware.os.lab.guts.api.AppendEntries;
import com.jivesoftware.os.lab.guts.api.AppendEntryStream;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.RawAppendableIndex;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.guts.api.Scanner;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author jonathan.colt
 */
public class LABMemoryIndex implements RawAppendableIndex {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final LABIndex index;
    private final AtomicLong approximateCount = new AtomicLong();
    private final AtomicBoolean disposed = new AtomicBoolean(false);

    private final ExecutorService destroy;
    private final LabHeapPressure labHeapPressure;
    private final AtomicLong costInBytes = new AtomicLong();

    private final Rawhide rawhide;

    private final int numBones = Short.MAX_VALUE;
    private final Semaphore hideABone = new Semaphore(numBones, true);
    private final ReadIndex reader;
    private AtomicReference<TimestampAndVersion> maxTimestampAndVersion = new AtomicReference<>(TimestampAndVersion.NULL);
    private final LABCostChangeInBytes costChangeInBytes;
    private final Compute compute;

    public LABMemoryIndex(ExecutorService destroy,
        LabHeapPressure labHeapPressure,
        Rawhide rawhide,
        LABIndex index) throws InterruptedException {

        this.costChangeInBytes = (cost) -> {
            costInBytes.addAndGet(cost);
            labHeapPressure.change(cost);
        };

        this.compute = (readKeyFormatTransformer, readValueFormatTransformer, rawEntryBuffer, value) -> {

            BolBuffer merged;
            if (value == null) {
                approximateCount.incrementAndGet();
                merged = rawEntryBuffer;
            } else {
                merged = rawhide.merge(FormatTransformer.NO_OP, FormatTransformer.NO_OP, value,
                    readKeyFormatTransformer, readValueFormatTransformer,
                    rawEntryBuffer, FormatTransformer.NO_OP, FormatTransformer.NO_OP);

            }
            long timestamp = rawhide.timestamp(FormatTransformer.NO_OP, FormatTransformer.NO_OP, merged);
            long version = rawhide.version(FormatTransformer.NO_OP, FormatTransformer.NO_OP, merged);
            updateMaxTimestampAndVersion(timestamp, version);
            return merged;
        };

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
                return index.scanner(from, to);
            }

            @Override
            public Scanner rowScan() throws Exception {
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

    public boolean containsKeyInRange(byte[] from, byte[] to) throws Exception {
        return index.contains(from, to);
    }

    @Override
    public boolean append(AppendEntries entries, BolBuffer keyBuffer) throws Exception {
        BolBuffer valueBuffer = new BolBuffer(); // Grrrr

        AppendEntryStream appendEntryStream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntryBuffer) -> {
            BolBuffer key = rawhide.key(readKeyFormatTransformer, readValueFormatTransformer, rawEntryBuffer, keyBuffer);
            index.compute(readKeyFormatTransformer, readValueFormatTransformer, rawEntryBuffer, key, valueBuffer, compute, costChangeInBytes);
            return true;
        };
        return entries.consume(appendEntryStream);
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
    public ReadIndex acquireReader() throws Exception {
        hideABone.acquire();
        if (disposed.get()) {
            hideABone.release();
            return null;
        }
        return reader;
    }

    public void destroy() throws Exception {
        destroy.submit(() -> {
            hideABone.acquire(numBones);
            try {
                disposed.set(true);
                index.clear();
                labHeapPressure.change(-costInBytes.get());
            } catch (Throwable t) {
                LOG.error("Destroy failed horribly!", t);
                throw t;
            } finally {
                hideABone.release(numBones);
            }
            return null;
        });
    }

    public void closeReadable() throws Exception {
    }

    @Override
    public void closeAppendable(boolean fsync) throws Exception {
    }

    public boolean isEmpty() throws Exception {
        return index.isEmpty();
    }

    public long count() {
        return approximateCount.get();
    }

    public long sizeInBytes() {
        return costInBytes.get();
    }

    public boolean mightContain(long newerThanTimestamp, long newerThanTimestampVersion) {
        TimestampAndVersion timestampAndVersion = maxTimestampAndVersion.get();
        return rawhide.mightContain(timestampAndVersion.maxTimestamp,
            timestampAndVersion.maxTimestampVersion,
            newerThanTimestamp,
            newerThanTimestampVersion);
    }

    public byte[] minKey() throws Exception {
        return index.firstKey();
    }

    public byte[] maxKey() throws Exception {
        return index.lastKey();
    }
}
