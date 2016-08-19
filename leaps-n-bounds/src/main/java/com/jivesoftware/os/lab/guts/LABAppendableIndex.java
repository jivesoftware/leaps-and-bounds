package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.RawEntryFormat;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.guts.api.AppendEntries;
import com.jivesoftware.os.lab.guts.api.RawAppendableIndex;
import com.jivesoftware.os.lab.io.AppendableHeap;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author jonathan.colt
 */
public class LABAppendableIndex implements RawAppendableIndex {

    public static final byte ENTRY = 0;
    public static final byte LEAP = 1;
    public static final byte FOOTER = 2;

    private final IndexRangeId indexRangeId;
    private final IndexFile index;
    private final int maxLeaps;
    private final int updatesBetweenLeaps;
    private final Rawhide rawhide;
    private final FormatTransformer writeKeyFormatTransormer;
    private final FormatTransformer writeValueFormatTransormer;
    private final RawEntryFormat rawhideFormat;

    private LeapFrog latestLeapFrog;
    private int updatesSinceLeap;

    private final long[] startOfEntryIndex;
    private byte[] firstKey;
    private byte[] lastKey;
    private int leapCount;
    private long count;
    private long keysSizeInBytes;
    private long valuesSizeInBytes;

    private long maxTimestamp = -1;
    private long maxTimestampVersion = -1;

    private volatile IAppendOnly appendOnly;

    public LABAppendableIndex(IndexRangeId indexRangeId,
        IndexFile index,
        int maxLeaps,
        int updatesBetweenLeaps,
        Rawhide rawhide,
        FormatTransformer writeKeyFormatTransormer,
        FormatTransformer writeValueFormatTransormer,
        RawEntryFormat rawhideFormat) {

        this.indexRangeId = indexRangeId;
        this.index = index;
        this.maxLeaps = maxLeaps;
        this.updatesBetweenLeaps = updatesBetweenLeaps;
        this.rawhide = rawhide;
        this.writeKeyFormatTransormer = writeKeyFormatTransormer;
        this.writeValueFormatTransormer = writeValueFormatTransormer;
        this.rawhideFormat = rawhideFormat;
        this.startOfEntryIndex = new long[updatesBetweenLeaps];
    }

    @Override
    public boolean append(AppendEntries appendEntries) throws Exception {
        if (appendOnly == null) {
            appendOnly = index.appender();
        }

        AppendableHeap appendBuffer = new AppendableHeap(1024);
        appendEntries.consume((readKeyFormatTransformer, readValueFormatTransformer, rawEntry, offset, length) -> {

            //entryBuffer.reset();
            long fp = appendOnly.getFilePointer();
            startOfEntryIndex[updatesSinceLeap] = fp + appendBuffer.length();
            UIO.writeByte(appendBuffer, ENTRY, "type");

            rawhide.writeRawEntry(readKeyFormatTransformer, readValueFormatTransformer, rawEntry, offset, length,
                writeKeyFormatTransormer, writeValueFormatTransormer, appendBuffer);

            byte[] key = rawhide.key(readKeyFormatTransformer, readValueFormatTransformer, rawEntry, offset, length);
            int keyLength = key.length;
            keysSizeInBytes += keyLength;
            valuesSizeInBytes += rawEntry.length - keyLength;

            ByteBuffer bbRawEntry = ByteBuffer.wrap(rawEntry);
            long rawEntryTimestamp = rawhide.timestamp(readKeyFormatTransformer, readValueFormatTransformer, bbRawEntry);
            if (rawEntryTimestamp > -1 && maxTimestamp < rawEntryTimestamp) {
                maxTimestamp = rawEntryTimestamp;
                maxTimestampVersion = rawhide.version(readKeyFormatTransformer, readValueFormatTransformer, bbRawEntry);
            } else {
                maxTimestamp = rawEntryTimestamp;
                maxTimestampVersion = rawhide.version(readKeyFormatTransformer, readValueFormatTransformer, bbRawEntry);
            }

            if (firstKey == null) {
                firstKey = key;
            }
            lastKey = key;
            updatesSinceLeap++;
            count++;

            if (updatesSinceLeap >= updatesBetweenLeaps) { // TODO consider bytes between leaps
                long[] copyOfStartOfEntryIndex = new long[updatesSinceLeap];
                System.arraycopy(startOfEntryIndex, 0, copyOfStartOfEntryIndex, 0, updatesSinceLeap);
                latestLeapFrog = writeLeaps(appendOnly, appendBuffer, latestLeapFrog, leapCount, key, copyOfStartOfEntryIndex);
                updatesSinceLeap = 0;
                leapCount++;

                appendOnly.append(appendBuffer.leakBytes(), 0, (int) appendBuffer.length());
                appendBuffer.reset();
            }
            return true;
        });

        if (appendBuffer.length() > 0) {
            appendOnly.append(appendBuffer.leakBytes(), 0, (int) appendBuffer.length());
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
                appendOnly = index.appender();
            }

            AppendableHeap appendableHeap = new AppendableHeap(8192);
            if (updatesSinceLeap > 0) {
                long[] copyOfStartOfEntryIndex = new long[updatesSinceLeap];
                System.arraycopy(startOfEntryIndex, 0, copyOfStartOfEntryIndex, 0, updatesSinceLeap);
                latestLeapFrog = writeLeaps(appendOnly, appendableHeap, latestLeapFrog, leapCount, lastKey, copyOfStartOfEntryIndex);
                leapCount++;
            }

            UIO.writeByte(appendableHeap, FOOTER, "type");
            Footer footer = new Footer(leapCount,
                count,
                keysSizeInBytes,
                valuesSizeInBytes,
                firstKey,
                lastKey,
                rawhideFormat.getKeyFormat(),
                rawhideFormat.getValueFormat(),
                new TimestampAndVersion(maxTimestamp,
                    maxTimestampVersion));
            footer.write(appendableHeap);

            appendOnly.append(appendableHeap.leakBytes(), 0, (int) appendableHeap.length());
            appendOnly.flush(fsync);
        } finally {
            close();
        }
    }

    public void close() throws IOException {
        index.close();
        if (appendOnly != null) {
            appendOnly.close();
        }
    }

    public void delete() {
        index.delete();
    }

    @Override
    public String toString() {
        return "LABAppendableIndex{"
            + "indexRangeId=" + indexRangeId
            + ", index=" + index
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
        byte[] key,
        long[] startOfEntryIndex) throws Exception {

        Leaps nextLeaps = LeapFrog.computeNextLeaps(index, ByteBuffer.wrap(key), latest, maxLeaps, startOfEntryIndex);
        UIO.writeByte(appendableHeap, LEAP, "type");
        long startOfLeapFp = appendableHeap.getFilePointer() + writeIndex.getFilePointer();
        nextLeaps.write(writeKeyFormatTransormer, appendableHeap);
        return new LeapFrog(startOfLeapFp, nextLeaps);
    }

}
