package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.FormatTransformerProvider;
import com.jivesoftware.os.lab.api.LABIndexCorruptedException;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.jivesoftware.os.lab.guts.LABAppendableIndex.FOOTER;
import static com.jivesoftware.os.lab.guts.LABAppendableIndex.LEAP;


/**
 * @author jonathan.colt
 */
public class ReadOnlyIndex {

    private static final AtomicLong CACHE_KEYS = new AtomicLong();
    private final IndexRangeId id;
    private final IndexFile index;
    private final ExecutorService destroy;
    private final AtomicBoolean disposed = new AtomicBoolean(false);
    private final LRUConcurrentBAHLinkedHash<Leaps> leapsCache;
    private final Footer footer;

    private final Semaphore hideABone;

    private final FormatTransformer readKeyFormatTransformer;
    private final FormatTransformer readValueFormatTransformer;
    private final Rawhide rawhide;
    private Leaps leaps; // loaded when reading
    private ReadLeapsAndBoundsIndex readLeapsAndBoundsIndex; // allocated when reading

    private final long cacheKey = CACHE_KEYS.incrementAndGet();

    public ReadOnlyIndex(ExecutorService destroy,
        IndexRangeId id,
        IndexFile index,
        FormatTransformerProvider formatTransformerProvider,
        Rawhide rawhide,
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache) throws Exception {
        this.destroy = destroy;
        this.id = id;
        this.index = index;
        this.hideABone = new Semaphore(Short.MAX_VALUE, true);
        long length = index.length();
        if (length == 0) {
            throw new LABIndexCorruptedException("Trying to construct an index with an empy file.");
        }
        IReadable reader = index.reader(null, length);
        this.footer = readFooter(reader);
        this.rawhide = rawhide;
        this.readKeyFormatTransformer = formatTransformerProvider.read(footer.keyFormat);
        this.readValueFormatTransformer = formatTransformerProvider.read(footer.valueFormat);
        this.leapsCache = leapsCache;
    }

    private Footer readFooter(IReadable readable) throws IOException, LABIndexCorruptedException {
        long indexLength = readable.length();
        long seekTo = indexLength - 4;
        if (seekTo < 0 || seekTo > indexLength) {
            throw new LABIndexCorruptedException(
                "1. Footer Corruption! trying to seek to: " + seekTo + " within file:" + index.getFileName() + " length:" + index.length());
        }
        readable.seek(indexLength - 4);
        int footerLength = readable.readInt();

        seekTo = indexLength - (1 + footerLength);
        if (seekTo < 0 || seekTo > indexLength) {
            throw new LABIndexCorruptedException(
                "2. Footer Corruption! trying to seek to: " + seekTo + " within file:" + index.getFileName() + " length:" + index.length());
        }
        readable.seek(seekTo);

        int type = readable.read();
        if (type != FOOTER) {
            throw new LABIndexCorruptedException(
                "4. Footer Corruption! Found " + type + " expected " + FOOTER + " within file:" + index.getFileName() + " length:" + index.length());
        }
        return Footer.read(readable);
    }

    public String name() {
        return id + " " + index.getFileName();
    }

    public IndexRangeId id() {
        return id;
    }

    // you must call release on this reader! Try to only use it as long have you have to!
    public ReadIndex acquireReader() throws Exception {
        hideABone.acquire();
        if (disposed.get() || index.isClosed()) {
            hideABone.release();
            return null;
        }

        try {
            if (leaps == null) {
                IReadable readableIndex = index.reader(null, index.length());
                long indexLength = readableIndex.length();

                long seekTo = indexLength - 4;
                if (seekTo < 0 || seekTo > indexLength) {
                    throw new LABIndexCorruptedException(
                        "1. Leaps Corruption! trying to seek to: " + seekTo + " within file:" + index.getFileName() + " length:" + index.length()
                    );
                }
                readableIndex.seek(seekTo);
                int footerLength = UIO.readInt(readableIndex, "length");
                seekTo = indexLength - (footerLength + 1 + 4);
                if (seekTo < 0 || seekTo > indexLength) {
                    throw new LABIndexCorruptedException(
                        "2. Leaps Corruption! trying to seek to: " + seekTo + " within file:" + index.getFileName() + " length:" + index.length()
                    );
                }
                readableIndex.seek(seekTo);
                int leapLength = UIO.readInt(readableIndex, "length");

                seekTo = indexLength - (1 + leapLength + 1 + footerLength);
                if (seekTo < 0 || seekTo > indexLength) {
                    throw new LABIndexCorruptedException(
                        "3. Leaps Corruption! trying to seek to: " + seekTo + " within file:" + index.getFileName() + " length:" + index.length()
                    );
                }
                readableIndex.seek(indexLength - (1 + leapLength + 1 + footerLength));

                int type = readableIndex.read();
                if (type != LEAP) {
                    throw new LABIndexCorruptedException(
                        "4. Leaps Corruption! " + type + " expected " + LEAP + " file:" + index.getFileName() + " length:" + index.length()
                    );
                }
                leaps = Leaps.read(readKeyFormatTransformer, readableIndex);
            }

            if (readLeapsAndBoundsIndex == null) {
                readLeapsAndBoundsIndex = new ReadLeapsAndBoundsIndex(hideABone,
                    rawhide,
                    readKeyFormatTransformer,
                    readValueFormatTransformer,
                    leaps,
                    cacheKey,
                    leapsCache,
                    footer,
                    () -> {
                        return index.reader(null, index.length());
                    }
                );
            }

            return readLeapsAndBoundsIndex;
        } catch (IOException | RuntimeException x) {
            hideABone.release();
            throw x;
        }
    }

    public void destroy() throws Exception {
        destroy.submit(() -> {

            hideABone.acquire(Short.MAX_VALUE);
            disposed.set(true);
            try {
                index.close();
                index.delete();
                //LOG.info("Destroyed {} {}", id, index.getFile());
            } finally {
                hideABone.release(Short.MAX_VALUE);
            }
            return null;
        });

    }

    public void flush(boolean fsync) throws Exception {
        hideABone.acquire();
        try {
            if (!disposed.get() && !index.isClosed()) {
                index.flush(fsync);
            }
        } finally {
            hideABone.release();
        }
    }

    public void closeReadable() throws Exception {
        hideABone.acquire(Short.MAX_VALUE);
        try {
            index.close();
        } finally {
            hideABone.release(Short.MAX_VALUE);
        }
    }

    public boolean isEmpty() throws IOException {
        return footer.count == 0;
    }

    public long count() throws IOException {
        return footer.count;
    }

    public long sizeInBytes() throws IOException {
        return index.length();
    }

    public long keysSizeInBytes() throws IOException {
        return footer.keysSizeInBytes;
    }

    public long valuesSizeInBytes() throws IOException {
        return footer.valuesSizeInBytes;
    }

    public byte[] minKey() {
        return footer.minKey;
    }

    public byte[] maxKey() {
        return footer.maxKey;
    }

    public Footer footer() {
        return footer;
    }

    public boolean containsKeyInRange(byte[] from, byte[] to) {
        Comparator<byte[]> keyComparator = rawhide.getKeyComparator();
        return keyComparator.compare(footer.minKey, from) <= 0 && keyComparator.compare(footer.maxKey, to) >= 0;
    }

    public String toString() {
        return "LeapsAndBoundsIndex{" + "id=" + id + ", index=" + index + ", disposed=" + disposed + ", footer=" + footer + '}';
    }

}
