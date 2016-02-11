package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.guts.api.RawAppendableIndex;
import com.jivesoftware.os.lab.guts.api.RawEntries;
import com.jivesoftware.os.lab.io.AppenableHeap;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.IOException;

/**
 * @author jonathan.colt
 */
public class LABAppenableIndex implements RawAppendableIndex {

    public static final byte ENTRY = 0;
    public static final byte LEAP = 1;
    public static final byte FOOTER = 2;

    private final IndexRangeId indexRangeId;
    private final IndexFile index;
    private final int maxLeaps;
    private final int updatesBetweenLeaps;
   
    private final byte[] lengthBuffer = new byte[4];

    private LeapFrog latestLeapFrog;
    private int updatesSinceLeap;

    private final long[] startOfEntryIndex;
    private byte[] firstKey;
    private byte[] lastKey;
    private int leapCount;
    private long count;

    private final IAppendOnly appendOnly;

    public LABAppenableIndex(IndexRangeId indexRangeId,
        IndexFile index,
        int maxLeaps,
        int updatesBetweenLeaps) throws IOException {
        
        this.indexRangeId = indexRangeId;
        this.index = index;
        this.maxLeaps = maxLeaps;
        this.updatesBetweenLeaps = updatesBetweenLeaps;
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

        AppenableHeap entryBuffer = new AppenableHeap(1024);
        rawEntries.consume((rawEntry, offset, length) -> {

            entryBuffer.reset();

            long fp = appendOnly.getFilePointer();
            startOfEntryIndex[updatesSinceLeap] = fp + 1 + 4;
            UIO.writeByte(entryBuffer, ENTRY, "type");
            int entryLength = 4 + length + 4;
            UIO.writeInt(entryBuffer, entryLength, "entryLength", lengthBuffer);
            entryBuffer.append(rawEntry, offset, length);
            UIO.writeInt(entryBuffer, entryLength, "entryLength", lengthBuffer);

            appendOnly.append(entryBuffer.leakBytes(), 0, (int) entryBuffer.length());

            int keyLength = UIO.bytesInt(rawEntry, offset);
            byte[] key = new byte[keyLength];
            System.arraycopy(rawEntry, 4, key, 0, keyLength);

            if (firstKey == null) {
                firstKey = key;
            }
            lastKey = key;
            updatesSinceLeap++;
            count++;

            if (updatesSinceLeap >= updatesBetweenLeaps) { // TODO consider bytes between leaps
                long[] copyOfStartOfEntryIndex = new long[updatesSinceLeap];
                System.arraycopy(startOfEntryIndex, 0, copyOfStartOfEntryIndex, 0, updatesSinceLeap);
                latestLeapFrog = writeLeaps(appendOnly, latestLeapFrog, leapCount, key, copyOfStartOfEntryIndex, lengthBuffer);
                updatesSinceLeap = 0;
                leapCount++;
            }
            return true;
        });

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
        if (updatesSinceLeap > 0) {
            long[] copyOfStartOfEntryIndex = new long[updatesSinceLeap];
            System.arraycopy(startOfEntryIndex, 0, copyOfStartOfEntryIndex, 0, updatesSinceLeap);
            latestLeapFrog = writeLeaps(appendOnly, latestLeapFrog, leapCount, lastKey, copyOfStartOfEntryIndex, lengthBuffer);
            leapCount++;
        }

        UIO.writeByte(appendOnly, FOOTER, "type");
        new Footer(leapCount, count, firstKey, lastKey).write(appendOnly, lengthBuffer);
        appendOnly.flush(fsync);
        index.close();
    }

    @Override
    public String toString() {
        return "WriteLeapsAndBoundsIndex{"
            + "indexRangeId=" + indexRangeId
            + ", index=" + index
            + ", maxLeaps=" + maxLeaps
            + ", updatesBetweenLeaps=" + updatesBetweenLeaps
            + ", count=" + count
            + '}';
    }

}
