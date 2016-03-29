package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.api.RawEntryMarshaller;
import com.jivesoftware.os.lab.guts.api.RawAppendableIndex;
import com.jivesoftware.os.lab.guts.api.RawEntries;
import com.jivesoftware.os.lab.io.AppendableHeap;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.IOException;

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
    private final RawEntryMarshaller mergeRawEntry;

    private final byte[] lengthBuffer = new byte[4];

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

    private final IAppendOnly appendOnly;

    public LABAppendableIndex(IndexRangeId indexRangeId,
        IndexFile index,
        int maxLeaps,
        int updatesBetweenLeaps,
        RawEntryMarshaller mergeRawEntry) throws IOException {

        this.indexRangeId = indexRangeId;
        this.index = index;
        this.maxLeaps = maxLeaps;
        this.updatesBetweenLeaps = updatesBetweenLeaps;
        this.mergeRawEntry = mergeRawEntry;
        this.startOfEntryIndex = new long[updatesBetweenLeaps];
        this.appendOnly = index.appender();
    }

    @Override
    public IndexRangeId id() {
        return indexRangeId;
    }

    public IndexFile getIndex() {
        return index;
    }

    @Override
    public boolean append(RawEntries rawEntries) throws Exception {

        AppendableHeap entryBuffer = new AppendableHeap(1024);
        rawEntries.consume((rawEntry, offset, length) -> {

            //entryBuffer.reset();
            long fp = appendOnly.getFilePointer();
            startOfEntryIndex[updatesSinceLeap] = fp + 1 + 4 + entryBuffer.length();
            UIO.writeByte(entryBuffer, ENTRY, "type");

            mergeRawEntry.writeRawEntry(rawEntry, offset, length, entryBuffer, lengthBuffer);
            byte[] key = mergeRawEntry.key(rawEntry, offset, length);
            int keyLength = key.length;
            keysSizeInBytes += keyLength;
            valuesSizeInBytes += rawEntry.length - keyLength;

            long rawEntryTimestamp = mergeRawEntry.timestamp(rawEntry, offset, length);
            if (rawEntryTimestamp > -1 && maxTimestamp < rawEntryTimestamp) {
                maxTimestamp = rawEntryTimestamp;
                maxTimestampVersion = mergeRawEntry.version(rawEntry, offset, length);
            } else {
                maxTimestamp = rawEntryTimestamp;
                maxTimestampVersion = mergeRawEntry.version(rawEntry, offset, length);
            }

            if (firstKey == null) {
                firstKey = key;
            }
            lastKey = key;
            updatesSinceLeap++;
            count++;

            if (updatesSinceLeap >= updatesBetweenLeaps) { // TODO consider bytes between leaps
                appendOnly.append(entryBuffer.leakBytes(), 0, (int) entryBuffer.length());
                long[] copyOfStartOfEntryIndex = new long[updatesSinceLeap];
                System.arraycopy(startOfEntryIndex, 0, copyOfStartOfEntryIndex, 0, updatesSinceLeap);
                latestLeapFrog = writeLeaps(appendOnly, latestLeapFrog, leapCount, key, copyOfStartOfEntryIndex, lengthBuffer);
                updatesSinceLeap = 0;
                leapCount++;

                entryBuffer.reset();
            }
            return true;
        });

        if (entryBuffer.length() > 0) {
            appendOnly.append(entryBuffer.leakBytes(), 0, (int) entryBuffer.length());
        }
        return true;
    }

    private LeapFrog writeLeaps(IAppendOnly writeIndex,
        LeapFrog latest,
        int index,
        byte[] key,
        long[] startOfEntryIndex,
        byte[] lengthBuffer) throws IOException {

        Leaps nextLeaps = LeapFrog.computeNextLeaps(index, key, latest, maxLeaps, startOfEntryIndex);
        UIO.writeByte(writeIndex, LEAP, "type");
        long startOfLeapFp = writeIndex.getFilePointer();
        nextLeaps.write(writeIndex, lengthBuffer);
        return new LeapFrog(startOfLeapFp, nextLeaps);
    }

    @Override
    public void closeAppendable(boolean fsync) throws IOException {
        if (firstKey == null || lastKey == null) {
            throw new IllegalStateException("Tried to close appendable index without a key range: " + this);
        }
        if (updatesSinceLeap > 0) {
            long[] copyOfStartOfEntryIndex = new long[updatesSinceLeap];
            System.arraycopy(startOfEntryIndex, 0, copyOfStartOfEntryIndex, 0, updatesSinceLeap);
            latestLeapFrog = writeLeaps(appendOnly, latestLeapFrog, leapCount, lastKey, copyOfStartOfEntryIndex, lengthBuffer);
            leapCount++;
        }

        UIO.writeByte(appendOnly, FOOTER, "type");
        new Footer(leapCount, count, keysSizeInBytes, valuesSizeInBytes, firstKey, lastKey, new TimestampAndVersion(maxTimestamp, maxTimestampVersion))
            .write(appendOnly, lengthBuffer);
        appendOnly.flush(fsync);
        index.close();
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

}
