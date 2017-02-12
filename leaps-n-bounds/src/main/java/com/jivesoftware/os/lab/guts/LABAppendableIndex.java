package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.FormatTransformerProvider;
import com.jivesoftware.os.lab.api.RawEntryFormat;
import com.jivesoftware.os.lab.api.rawhide.Rawhide;
import com.jivesoftware.os.lab.guts.api.AppendEntries;
import com.jivesoftware.os.lab.guts.api.RawAppendableIndex;
import com.jivesoftware.os.lab.io.AppendableHeap;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.PointerReadableByteBufferFile;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author jonathan.colt
 */
public class LABAppendableIndex implements RawAppendableIndex {

    public static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public static final byte ENTRY = 0;
    public static final byte LEAP = 1;
    public static final byte FOOTER = 2;

    private final LongAdder appendedStat;
    private final IndexRangeId indexRangeId;
    private final AppendOnlyFile appendOnlyFile;
    private final int maxLeaps;
    private final int updatesBetweenLeaps;
    private final Rawhide rawhide;
    private final FormatTransformerProvider formatTransformerProvider;
    private final FormatTransformer writeKeyFormatTransormer;
    private final FormatTransformer writeValueFormatTransormer;
    private final RawEntryFormat rawhideFormat;
    private final double hashIndexLoadFactor;

    private LeapFrog latestLeapFrog;
    private int updatesSinceLeap;

    private final long[] startOfEntryIndex;
    private BolBuffer firstKey;
    private BolBuffer lastKey;
    private int leapCount;
    private long count;
    private long keysSizeInBytes;
    private long valuesSizeInBytes;

    private long maxTimestamp = -1;
    private long maxTimestampVersion = -1;

    private volatile IAppendOnly appendOnly;

    public LABAppendableIndex(LongAdder appendedStat,
        IndexRangeId indexRangeId,
        AppendOnlyFile appendOnlyFile,
        int maxLeaps,
        int updatesBetweenLeaps,
        Rawhide rawhide,
        RawEntryFormat rawhideFormat,
        FormatTransformerProvider formatTransformerProvider,
        double hashIndexLoadFactor) throws Exception {

        this.appendedStat = appendedStat;
        this.indexRangeId = indexRangeId;
        this.appendOnlyFile = appendOnlyFile;
        this.maxLeaps = maxLeaps;
        this.updatesBetweenLeaps = updatesBetweenLeaps;
        this.rawhide = rawhide;
        this.formatTransformerProvider = formatTransformerProvider;
        this.rawhideFormat = rawhideFormat;
        this.hashIndexLoadFactor = hashIndexLoadFactor;

        this.writeKeyFormatTransormer = formatTransformerProvider.write(rawhideFormat.getKeyFormat());
        this.writeValueFormatTransormer = formatTransformerProvider.write(rawhideFormat.getValueFormat());
        this.startOfEntryIndex = new long[updatesBetweenLeaps];
    }

    @Override
    public boolean append(AppendEntries appendEntries, BolBuffer keyBuffer) throws Exception {
        if (appendOnly == null) {
            appendOnly = appendOnlyFile.appender();
        }
        AppendableHeap appendableHeap = new AppendableHeap(1024);
        appendEntries.consume((readKeyFormatTransformer, readValueFormatTransformer, rawEntryBuffer) -> {

            //entryBuffer.reset();
            long fp = appendOnly.getFilePointer();
            startOfEntryIndex[updatesSinceLeap] = fp + appendableHeap.length();
            appendableHeap.appendByte(ENTRY);

            rawhide.writeRawEntry(readKeyFormatTransformer, readValueFormatTransformer, rawEntryBuffer,
                writeKeyFormatTransormer, writeValueFormatTransormer, appendableHeap);

            BolBuffer key = rawhide.key(readKeyFormatTransformer, readValueFormatTransformer, rawEntryBuffer, keyBuffer);
            int keyLength = key.length;
            keysSizeInBytes += keyLength;
            valuesSizeInBytes += rawEntryBuffer.length - keyLength;

            long rawEntryTimestamp = rawhide.timestamp(readKeyFormatTransformer, readValueFormatTransformer, rawEntryBuffer);
            if (rawEntryTimestamp > -1 && maxTimestamp < rawEntryTimestamp) {
                maxTimestamp = rawEntryTimestamp;
                maxTimestampVersion = rawhide.version(readKeyFormatTransformer, readValueFormatTransformer, rawEntryBuffer);
            } else {
                maxTimestamp = rawEntryTimestamp;
                maxTimestampVersion = rawhide.version(readKeyFormatTransformer, readValueFormatTransformer, rawEntryBuffer);
            }

            if (firstKey == null) {
                firstKey = new BolBuffer();
                firstKey.set(key);
            }
            if (lastKey == null) {
                lastKey = new BolBuffer();
            }
            lastKey.set(key);
            updatesSinceLeap++;
            count++;

            if (updatesSinceLeap >= updatesBetweenLeaps) { // TODO consider bytes between leaps
                long[] copyOfStartOfEntryIndex = new long[updatesSinceLeap];
                System.arraycopy(startOfEntryIndex, 0, copyOfStartOfEntryIndex, 0, updatesSinceLeap);
                latestLeapFrog = writeLeaps(appendOnly, appendableHeap, latestLeapFrog, leapCount, key, copyOfStartOfEntryIndex);
                updatesSinceLeap = 0;
                leapCount++;

                appendOnly.append(appendableHeap.leakBytes(), 0, (int) appendableHeap.length());
                appendedStat.add(appendableHeap.length());
                appendableHeap.reset();
            }
            return true;
        });

        if (appendableHeap.length() > 0) {
            appendOnly.append(appendableHeap.leakBytes(), 0, (int) appendableHeap.length());
            appendedStat.add(appendableHeap.length());
        }
        return true;
    }

    @Override
    public void closeAppendable(boolean fsync) throws Exception {
        try {

            if (firstKey == null || lastKey == null) {
                throw new IllegalStateException("Tried to close appendable index without a key range: " + this);
            }

            if (appendOnly == null) {
                appendOnly = appendOnlyFile.appender();
            }

            AppendableHeap appendableHeap = new AppendableHeap(8192);
            if (updatesSinceLeap > 0) {
                long[] copyOfStartOfEntryIndex = new long[updatesSinceLeap];
                System.arraycopy(startOfEntryIndex, 0, copyOfStartOfEntryIndex, 0, updatesSinceLeap);
                latestLeapFrog = writeLeaps(appendOnly, appendableHeap, latestLeapFrog, leapCount, lastKey, copyOfStartOfEntryIndex);
                leapCount++;
            }

            appendableHeap.appendByte(FOOTER);
            Footer footer = new Footer(leapCount,
                count,
                keysSizeInBytes,
                valuesSizeInBytes,
                firstKey.copy(),
                lastKey.copy(),
                rawhideFormat.getKeyFormat(),
                rawhideFormat.getValueFormat(),
                maxTimestamp,
                maxTimestampVersion);
            footer.write(appendableHeap);

            appendOnly.append(appendableHeap.leakBytes(), 0, (int) appendableHeap.length());
            appendedStat.add(appendableHeap.length());
            appendOnly.flush(fsync);
        } finally {
            close();
        }

        buildHashIndex(count); // HACKY :(
    }


    // TODO this could / should be rewritten to reduce seek thrashing by using batching.
    private void buildHashIndex(long count) throws Exception {
        if (hashIndexLoadFactor > 0) {
            long[] runHisto = new long[33];
            FormatTransformer readKeyFormatTransformer = formatTransformerProvider.read(rawhideFormat.getKeyFormat());
            FormatTransformer readValueFormatTransformer = formatTransformerProvider.read(rawhideFormat.getValueFormat());

            RandomAccessFile f = new RandomAccessFile(appendOnlyFile.getFile(), "rw");
            long length = f.length();

            int chunkPower = UIO.chunkPower(length + 1, 0);
            byte hashIndexLongPrecision;
            if (chunkPower < 8) {
                hashIndexLongPrecision = 1;
            } else if (chunkPower < 16) {
                hashIndexLongPrecision = 2;
            } else if (chunkPower < 24) {
                hashIndexLongPrecision = 3;
            } else if (chunkPower < 32) {
                hashIndexLongPrecision = 4;
            } else if (chunkPower < 40) {
                hashIndexLongPrecision = 5;
            } else if (chunkPower < 48) {
                hashIndexLongPrecision = 6;
            } else if (chunkPower < 56) {
                hashIndexLongPrecision = 7;
            } else {
                hashIndexLongPrecision = 8;
            }

            long hashIndexMaxCapacity = count + (long) (count * hashIndexLoadFactor);
            long hashIndexSizeInBytes = hashIndexMaxCapacity * (hashIndexLongPrecision + 1);
            f.setLength(length + hashIndexSizeInBytes + 1 + 8 + 4);

            PointerReadableByteBufferFile c = new PointerReadableByteBufferFile(ReadOnlyFile.BUFFER_SEGMENT_SIZE, appendOnlyFile.getFile(), true);

            long start = System.currentTimeMillis();
            long clear = 0;
            int worstRun = 0;

            try {
                long offset = length;
                for (int i = 0; i < hashIndexMaxCapacity; i++) {
                    c.writeVPLong(offset, 0, hashIndexLongPrecision);
                    offset += hashIndexLongPrecision;
                    c.write(offset, (byte) 0);
                    offset++;
                }
                c.write(offset, (byte) hashIndexLongPrecision);
                offset++;
                c.writeLong(offset, hashIndexMaxCapacity);
                offset += 8;
                c.writeInt(offset, -1);

                long time = System.currentTimeMillis();
                clear = time - start;
                start = time;


                BolBuffer key = new BolBuffer();
                BolBuffer entryBuffer = new BolBuffer();

                long activeOffset = 0;

                int batchSize = 1024 * 10;
                int batchCount = 0;
                long[] hashIndexes = new long[batchSize];
                long[] startOfEntryOffsets = new long[batchSize];

                NEXT_ENTRY:
                while (true) {
                    int type = c.read(activeOffset);
                    activeOffset++;

                    if (type == ENTRY) {
                        long startOfEntryOffset = activeOffset;
                        activeOffset += rawhide.rawEntryToBuffer(c, activeOffset, entryBuffer);

                        BolBuffer k = rawhide.key(readKeyFormatTransformer, readValueFormatTransformer, entryBuffer, key);

                        long hashIndex = k.longHashCode() % hashIndexMaxCapacity;

                        hashIndexes[batchCount] = hashIndex;
                        startOfEntryOffsets[batchCount] = startOfEntryOffset;
                        batchCount++;

                        if (batchCount == batchSize) {
                            int maxRun = hash(runHisto, length, hashIndexMaxCapacity, hashIndexLongPrecision, c, startOfEntryOffsets, hashIndexes, batchCount);
                            worstRun = Math.max(maxRun,worstRun);
                            batchCount = 0;
                        }
                        continue NEXT_ENTRY;
                    } else if (type == FOOTER) {
                        break;
                    } else if (type == LEAP) {
                        activeOffset += c.readInt(activeOffset);
                    } else {
                        throw new IllegalStateException("Bad row type:" + type + " at fp:" + (activeOffset - 1));
                    }
                }
                if (batchCount > 0) {
                    int maxRun = hash(runHisto, length, hashIndexMaxCapacity, hashIndexLongPrecision, c, startOfEntryOffsets, hashIndexes, batchCount);
                    worstRun = Math.max(maxRun,worstRun);
                }
                f.getFD().sync();

            } finally {
                c.close();
                f.close();
            }

            LOG.info("Built hash index for {} entries in {} + {} millis precision: {} cost: {} bytes worstRun:{}",
                count,
                clear,
                System.currentTimeMillis() - start,
                hashIndexLongPrecision,
                hashIndexSizeInBytes,
                worstRun);

            for (int i = 0; i < 32; i++) {
                if (runHisto[i] > 0) {
                    LOG.inc("write>runs>" + i, runHisto[i]);
                }
            }
            if (runHisto[32] > 0) {
                LOG.inc("write>runs>horrible", runHisto[32]);
            }
        }
    }

    private int hash(long[] runHisto,
        long length,
        long hashIndexMaxCapacity,
        byte hashIndexLongPrecision,
        PointerReadableByteBufferFile c,
        long[] startOfEntryOffset,
        long[] hashIndex,
        int count) throws IOException {

        int worstRun = 0;
        NEXT:
        for (int i = 0; i < count; i++) {
            long hi = hashIndex[i];
            int r = 0;
            while (r < hashIndexMaxCapacity) {
                long pos = length + (hi * (hashIndexLongPrecision + 1));
                long v = c.readVPLong(pos, hashIndexLongPrecision);
                if (v == 0) {
                    c.writeVPLong(pos, startOfEntryOffset[i] + 1, hashIndexLongPrecision); // +1 so 0 can be null
                    worstRun = Math.max(r,worstRun);
                    if (r < 32) {
                        runHisto[r]++;
                    } else {
                        runHisto[32]++;
                    }
                    continue NEXT;
                } else {
                    c.write(pos + hashIndexLongPrecision, (byte) 1);
                    r++;
                    hi = (++hi) % hashIndexMaxCapacity;
                }
            }
            throw new IllegalStateException("WriteHashIndex failed to add entry because there was no free slot.");
        }
        return worstRun;
    }

    public void close() throws IOException {
        appendOnlyFile.close();
        if (appendOnly != null) {
            appendOnly.close();
        }
    }

    public void delete() {
        appendOnlyFile.delete();
    }

    @Override
    public String toString() {
        return "LABAppendableIndex{"
            + "indexRangeId=" + indexRangeId
            + ", index=" + appendOnlyFile
            + ", maxLeaps=" + maxLeaps
            + ", updatesBetweenLeaps=" + updatesBetweenLeaps
            + ", updatesSinceLeap=" + updatesSinceLeap
            + ", leapCount=" + leapCount
            + ", count=" + count
            + '}';
    }

    private LeapFrog writeLeaps(IAppendOnly writeIndex,
        IAppendOnly appendableHeap,
        LeapFrog latest,
        int index,
        BolBuffer key,
        long[] startOfEntryIndex) throws Exception {

        Leaps nextLeaps = LeapFrog.computeNextLeaps(index, key, latest, maxLeaps, startOfEntryIndex);
        appendableHeap.appendByte(LEAP);
        long startOfLeapFp = appendableHeap.getFilePointer() + writeIndex.getFilePointer();
        nextLeaps.write(writeKeyFormatTransormer, appendableHeap);
        return new LeapFrog(startOfLeapFp, nextLeaps);
    }


}
