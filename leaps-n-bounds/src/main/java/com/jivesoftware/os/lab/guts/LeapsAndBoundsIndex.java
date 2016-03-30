package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.jive.utils.collections.lh.ConcurrentLHash;
import com.jivesoftware.os.lab.api.LABIndexCorruptedException;
import com.jivesoftware.os.lab.api.Rawhide;
import com.jivesoftware.os.lab.guts.api.RawConcurrentReadableIndex;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.lab.io.api.UIO;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.jivesoftware.os.lab.guts.LABAppendableIndex.FOOTER;
import static com.jivesoftware.os.lab.guts.LABAppendableIndex.LEAP;

/**
 * @author jonathan.colt
 */
public class LeapsAndBoundsIndex implements RawConcurrentReadableIndex {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final IndexRangeId id;
    private final IndexFile index;
    private final ExecutorService destroy;
    private final AtomicBoolean disposed = new AtomicBoolean(false);
    private final ConcurrentLHash<Leaps> leapsCache;
    private final Footer footer;

    private final int numBonesHidden = Short.MAX_VALUE; // TODO config
    private final Semaphore hideABone;

    private final Rawhide rawhide;
    private Leaps leaps; // loaded when reading

    public LeapsAndBoundsIndex(ExecutorService destroy, IndexRangeId id, IndexFile index, Rawhide rawhide, int concurrency) throws Exception {
        this.destroy = destroy;
        this.id = id;
        this.index = index;
        this.hideABone = new Semaphore(numBonesHidden, true);
        long length = index.length();
        if (length == 0) {
            throw new LABIndexCorruptedException("Trying to construct an index with an empy file.");
        }
        IReadable reader = index.reader(null, length, true);
        this.footer = readFooter(reader);
        this.rawhide = rawhide;
        this.leapsCache = new ConcurrentLHash<>(3, -2, -1, concurrency); // TODO config
    }

    private Footer readFooter(IReadable readable) throws IOException, LABIndexCorruptedException {
        byte[] lengthBuffer = new byte[8];
        long indexLength = readable.length();
        long seekTo = indexLength - 4;
        if (seekTo < 0 || seekTo > indexLength) {
            throw new LABIndexCorruptedException(
                "1. Footer Corruption! trying to seek to: " + seekTo + " within file:" + index.getFile() + " length:" + index.length());
        }
        readable.seek(indexLength - 4);
        int footerLength = UIO.readInt(readable, "length", lengthBuffer);

        seekTo = indexLength - (1 + footerLength);
        if (seekTo < 0 || seekTo > indexLength) {
            throw new LABIndexCorruptedException(
                "2. Footer Corruption! trying to seek to: " + seekTo + " within file:" + index.getFile() + " length:" + index.length());
        }
        readable.seek(seekTo);

        int type = readable.read();
        if (type != FOOTER) {
            throw new LABIndexCorruptedException(
                "4. Footer Corruption! Found " + type + " expected " + FOOTER + " within file:" + index.getFile() + " length:" + index.length());
        }
        return Footer.read(readable, lengthBuffer);
    }

    @Override
    public String name() {
        return id + " " + index.getFile();
    }

    @Override
    public IndexRangeId id() {
        return id;
    }

    // you must call release on this reader! Try to only use it as long have you have to!
    @Override
    public ReadIndex acquireReader() throws Exception {
        hideABone.acquire();
        if (disposed.get()) {
            hideABone.release();
            return null;
        }

        try {
            IReadable readableIndex = index.reader(null, index.length(), false);

            if (leaps == null) {
                long indexLength = readableIndex.length();
                byte[] lengthBuffer = new byte[8];

                long seekTo = indexLength - 4;
                if (seekTo < 0 || seekTo > indexLength) {
                    throw new RuntimeException("1. Leaps Corruption! trying to seek to: " + seekTo + " within file:" + index.getFile() + " length:" + index
                        .length());
                }
                readableIndex.seek(seekTo);
                int footerLength = UIO.readInt(readableIndex, "length", lengthBuffer);
                seekTo = indexLength - (footerLength + 1 + 4);
                if (seekTo < 0 || seekTo > indexLength) {
                    throw new RuntimeException("2. Leaps Corruption! trying to seek to: " + seekTo + " within file:" + index.getFile() + " length:" + index
                        .length());
                }
                readableIndex.seek(seekTo);
                int leapLength = UIO.readInt(readableIndex, "length", lengthBuffer);

                seekTo = indexLength - (1 + leapLength + 1 + footerLength);
                if (seekTo < 0 || seekTo > indexLength) {
                    throw new RuntimeException("3. Leaps Corruption! trying to seek to: " + seekTo + " within file:" + index.getFile() + " length:" + index
                        .length());
                }
                readableIndex.seek(indexLength - (1 + leapLength + 1 + footerLength));

                int type = readableIndex.read();
                if (type != LEAP) {
                    throw new RuntimeException("4. Leaps Corruption! " + type + " expected " + LEAP + " file:" + index.getFile() + " length:" + index.length());
                }
                leaps = Leaps.read(readableIndex, lengthBuffer);
            }
            ReadLeapsAndBoundsIndex i = new ReadLeapsAndBoundsIndex(hideABone, rawhide, leaps, leapsCache, footer, () -> {
                return index.reader(null, index.length(), false);
            });

            return i;
        } catch (IOException | RuntimeException x) {
            hideABone.release();
            throw x;
        }
    }

    @Override
    public void destroy() throws Exception {
        destroy.submit(() -> {

            hideABone.acquire(numBonesHidden);
            disposed.set(true);
            try {
                index.close();
                index.getFile().delete();
                //LOG.info("Destroyed {} {}", id, index.getFile());
            } finally {
                hideABone.release(numBonesHidden);
            }
            return null;
        });

    }

    public void flush(boolean fsync) throws Exception {
        index.appender().flush(fsync);
    }

    @Override
    public void closeReadable() throws Exception {
        hideABone.acquire(numBonesHidden);
        try {
            index.close();
        } finally {
            hideABone.release(numBonesHidden);
        }
    }

    @Override
    public boolean isEmpty() throws IOException {
        return footer.count == 0;
    }

    @Override
    public long count() throws IOException {
        return footer.count;
    }

    @Override
    public long sizeInBytes() throws IOException {
        return index.length();
    }

    @Override
    public long keysSizeInBytes() throws IOException {
        return footer.keysSizeInBytes;
    }

    @Override
    public long valuesSizeInBytes() throws IOException {
        return footer.valuesSizeInBytes;
    }

    @Override
    public byte[] minKey() {
        return footer.minKey;
    }

    @Override
    public byte[] maxKey() {
        return footer.maxKey;
    }

    @Override
    public TimestampAndVersion maxTimestampAndVersion() {
        return footer.maxTimestampAndVersion;
    }

    @Override
    public String toString() {
        return "LeapsAndBoundsIndex{" + "id=" + id + ", index=" + index + ", disposed=" + disposed + ", footer=" + footer + '}';
    }

}
