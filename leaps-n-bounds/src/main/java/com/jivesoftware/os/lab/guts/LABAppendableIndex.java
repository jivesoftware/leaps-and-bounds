package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.FormatTransformerProvider;
import com.jivesoftware.os.lab.api.RawEntryFormat;
import com.jivesoftware.os.lab.api.rawhide.Rawhide;
import com.jivesoftware.os.lab.guts.api.AppendEntries;
import com.jivesoftware.os.lab.guts.api.RawAppendableIndex;
import com.jivesoftware.os.lab.io.AppendableHeap;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.IPointerReadable;
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
            long start = System.currentTimeMillis();
            try {
                long length = f.length();
                long hashIndexMaxCapacity = count + (long) (count * hashIndexLoadFactor);
                long hashIndexSizeInBytes = hashIndexMaxCapacity * (8 + 1);
                f.setLength(length + hashIndexSizeInBytes + 8 + 4);
                f.seek(length);
                for (int i = 0; i < hashIndexMaxCapacity; i++) {
                    f.writeLong(-1L);
                    f.write(0);
                }
                f.writeLong(hashIndexMaxCapacity);
                f.writeInt(-1);

                BolBuffer key = new BolBuffer();
                BolBuffer entryBuffer = new BolBuffer();

                RandomAccessBackPointerReadable readable = new RandomAccessBackPointerReadable(f);
                long activeOffset = 0;

                NEXT_ENTRY:
                while (true) {
                    int type = readable.read(activeOffset);
                    activeOffset++;

                    if (type == ENTRY) {
                        long startOfEntryOffset = activeOffset;
                        activeOffset += rawhide.rawEntryToBuffer(readable, activeOffset, entryBuffer);

                        BolBuffer k = rawhide.key(readKeyFormatTransformer, readValueFormatTransformer, entryBuffer, key);

                        long hashIndex = k.longHashCode() % hashIndexMaxCapacity;

                        int i = 0;
                        while (i < hashIndexMaxCapacity) {
                            long pos = length + (hashIndex * (8 + 1));
                            f.seek(pos);
                            long v = f.readLong();
                            if (v == -1) {
                                f.seek(pos);
                                f.writeLong(startOfEntryOffset);

                                if (i < 32) {
                                    runHisto[i]++;
                                } else {
                                    runHisto[32]++;
                                }
                                continue NEXT_ENTRY;
                            } else {
                                f.write(1);
                                i++;
                                hashIndex = (++hashIndex) % hashIndexMaxCapacity;
                            }
                        }
                        throw new IllegalStateException("WriteHashIndex failed to add entry because there was no free slot.");


                    } else if (type == FOOTER) {
                        break;
                    } else if (type == LEAP) {
                        activeOffset += f.readInt();
                    } else {
                        throw new IllegalStateException("Bad row type:" + type + " at fp:" + (activeOffset - 1));
                    }
                }
                f.getFD().sync();


                /*String name = appendOnlyFile.getFile().getName();
                System.out.println(">>>indexBegin " + name);
                BolBuffer peek = new BolBuffer();
                f.seek(length);
                for (int i = 0; i < hashIndexMaxCapacity; i++) {
                    long pos = length + (i * 9);
                    f.seek(pos);
                    long v = f.readLong();
                    int run = f.read();
                    peek.allocate(0);
                    if (v != -1) {
                        rawhide.rawEntryToBuffer(readable, v, entryBuffer);
                        BolBuffer k = rawhide.key(readKeyFormatTransformer, readValueFormatTransformer, entryBuffer, peek);
                        System.out.println(pos + " index: " + name + " " + v + " " + run + " " + Arrays.toString(k.copy()));
                    } else {
                        System.out.println(pos + " index: " + name + " " + v + " " + run);
                    }

                }
                System.out.println("<<<<indexEnd " + name);*/


            } finally {
                f.close();
            }

            LOG.info("Built hash index for {} entries in {} millis", count, System.currentTimeMillis() - start);

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

    private class RandomAccessBackPointerReadable implements IPointerReadable {
        private final RandomAccessFile f;

        private RandomAccessBackPointerReadable(RandomAccessFile f) {
            this.f = f;
        }

        @Override
        public long length() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(long readPointer) throws IOException {
            f.seek(readPointer);
            return f.read();
        }

        @Override
        public int readInt(long readPointer) throws IOException {
            f.seek(readPointer);
            return f.readInt();
        }

        @Override
        public long readLong(long readPointer) throws IOException {
            f.seek(readPointer);
            return f.readLong();
        }

        @Override
        public int read(long readPointer, byte[] b, int _offset, int _len) throws IOException {
            f.seek(readPointer);
            return f.read(b, _offset, _len);
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public BolBuffer sliceIntoBuffer(long readPointer, int length, BolBuffer entryBuffer) throws IOException {
            entryBuffer.allocate(length);
            read(readPointer, entryBuffer.bytes, 0, length);
            return null;
        }
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
