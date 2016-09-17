package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.jive.utils.collections.bah.LRUConcurrentBAHLinkedHash;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.FormatTransformerProvider;
import com.jivesoftware.os.lab.api.exceptions.LABCorruptedException;
import com.jivesoftware.os.lab.api.rawhide.Rawhide;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.io.api.IPointerReadable;
import java.io.IOException;
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
    private final ReadOnlyFile readOnlyFile;
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
        ReadOnlyFile readOnlyFile,
        FormatTransformerProvider formatTransformerProvider,
        Rawhide rawhide,
        LRUConcurrentBAHLinkedHash<Leaps> leapsCache) throws Exception {
        this.destroy = destroy;
        this.id = id;
        this.readOnlyFile = readOnlyFile;
        this.hideABone = new Semaphore(Short.MAX_VALUE, true);
        long length = readOnlyFile.length();
        if (length == 0) {
            throw new LABCorruptedException("Trying to construct an index with an empy file.");
        }
        this.footer = readFooter(readOnlyFile.pointerReadable(-1));
        this.rawhide = rawhide;
        this.readKeyFormatTransformer = formatTransformerProvider.read(footer.keyFormat);
        this.readValueFormatTransformer = formatTransformerProvider.read(footer.valueFormat);
        this.leapsCache = leapsCache;
    }

    private Footer readFooter(IPointerReadable readable) throws IOException, LABCorruptedException {
        long indexLength = readable.length();
        long seekTo = indexLength - 4;
        if (seekTo < 0 || seekTo > indexLength) {
            throw new LABCorruptedException(
                "1. Footer Corruption! trying to seek to: " + seekTo + " within file:" + readOnlyFile.getFileName() + " length:" + readOnlyFile.length());
        }
        int footerLength = readable.readInt(indexLength - 4);

        seekTo = indexLength - (1 + footerLength);
        if (seekTo < 0 || seekTo > indexLength) {
            throw new LABCorruptedException(
                "2. Footer Corruption! trying to seek to: " + seekTo + " within file:" + readOnlyFile.getFileName() + " length:" + readOnlyFile.length());
        }

        int type = readable.read(seekTo);
        seekTo++;
        if (type != FOOTER) {
            throw new LABCorruptedException(
                "4. Footer Corruption! Found " + type + " expected " + FOOTER + " within file:" + readOnlyFile.getFileName() + " length:" + readOnlyFile.length());
        }
        return Footer.read(readable, seekTo);
    }

    public String name() {
        return id + " " + readOnlyFile.getFileName();
    }

    public IndexRangeId id() {
        return id;
    }

    // you must call release on this reader! Try to only use it as long have you have to!
    public ReadIndex acquireReader() throws Exception {
        hideABone.acquire();
        if (disposed.get() || readOnlyFile.isClosed()) {
            hideABone.release();
            return null;
        }

        try {
            if (leaps == null) {
                IPointerReadable readableIndex = readOnlyFile.pointerReadable(-1);
                long indexLength = readableIndex.length();

                long offset = indexLength - 4;
                if (offset < 0 || offset > indexLength) {
                    throw new LABCorruptedException(
                        "1. Leaps Corruption! trying to seek to: " + offset + " within file:" + readOnlyFile.getFileName() + " length:" + readOnlyFile.length()
                    );
                }
                int footerLength = readableIndex.readInt(offset);
                offset = indexLength - (footerLength + 1 + 4);
                if (offset < 0 || offset > indexLength) {
                    throw new LABCorruptedException(
                        "2. Leaps Corruption! trying to seek to: " + offset + " within file:" + readOnlyFile.getFileName() + " length:" + readOnlyFile.length()
                    );
                }
                int leapLength = readableIndex.readInt(offset);

                offset = indexLength - (1 + leapLength + 1 + footerLength);
                if (offset < 0 || offset > indexLength) {
                    throw new LABCorruptedException(
                        "3. Leaps Corruption! trying to seek to: " + offset + " within file:" + readOnlyFile.getFileName() + " length:" + readOnlyFile.length()
                    );
                }
                offset = indexLength - (1 + leapLength + 1 + footerLength);

                int type = readableIndex.read(offset);
                offset++;
                if (type != LEAP) {
                    throw new LABCorruptedException(
                        "4. Leaps Corruption! " + type + " expected " + LEAP + " file:" + readOnlyFile.getFileName() + " length:" + readOnlyFile.length()
                    );
                }
                leaps = Leaps.read(readKeyFormatTransformer, readableIndex, offset);
            }

            if (readLeapsAndBoundsIndex == null) {
                readLeapsAndBoundsIndex = new ReadLeapsAndBoundsIndex(hideABone,
                    rawhide,
                    footer,
                    () -> {

                        ActiveScan activeScan = new ActiveScan(rawhide,
                            readKeyFormatTransformer,
                            readValueFormatTransformer,
                            leaps,
                            cacheKey,
                            leapsCache,
                            footer,
                            readOnlyFile.pointerReadable(-1),
                            new byte[16]);
                        return activeScan;
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
                readOnlyFile.close();
                readOnlyFile.delete();
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
            if (!disposed.get() && !readOnlyFile.isClosed()) {
                readOnlyFile.flush(fsync);
            }
        } finally {
            hideABone.release();
        }
    }

    public void closeReadable() throws Exception {
        hideABone.acquire(Short.MAX_VALUE);
        try {
            readOnlyFile.close();
        } finally {
            hideABone.release(Short.MAX_VALUE);
        }
    }

    public long count() throws IOException {
        return footer.count;
    }

    public long sizeInBytes() throws IOException {
        return readOnlyFile.length();
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

    @Override
    public String toString() {
        return "LeapsAndBoundsIndex{" + "id=" + id + ", index=" + readOnlyFile + ", disposed=" + disposed + ", footer=" + footer + '}';
    }

}
