package com.jivesoftware.os.lab;

import com.jivesoftware.os.lab.api.RawConcurrentReadableIndex;
import com.jivesoftware.os.lab.api.ReadIndex;
import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.lab.io.api.UIO;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.jivesoftware.os.lab.WriteLeapsAndBoundsIndex.FOOTER;
import static com.jivesoftware.os.lab.WriteLeapsAndBoundsIndex.LEAP;

/**
 * @author jonathan.colt
 */
public class LeapsAndBoundsIndex implements RawConcurrentReadableIndex {

    private final byte[] lengthBuffer = new byte[8];
    private final IndexRangeId id;
    private final IndexFile index;
    private final ExecutorService destroy;
    private final AtomicBoolean disposed = new AtomicBoolean(false);

    private final int numBonesHidden = 1024; // TODO config
    private final Semaphore hideABone;

    public LeapsAndBoundsIndex(ExecutorService destroy, IndexRangeId id, IndexFile index) throws Exception {
        this.destroy = destroy;
        this.id = id;
        this.index = index;
        this.hideABone = new Semaphore(numBonesHidden, true);
        this.footer = readFooter(index.reader(null, index.length(), 0));
    }

    private Footer readFooter(IReadable readable) throws IOException {
        long indexLength = readable.length();
        if (indexLength < 4) {
            System.out.println("WTF:" + indexLength);
        }

        readable.seek(indexLength - 4);
        int footerLength = UIO.readInt(readable, "length", lengthBuffer);
        readable.seek(indexLength - (1 + footerLength));

        int type = readable.read();
        if (type != FOOTER) {
            throw new RuntimeException("Corruption! " + type + " expected " + FOOTER);
        }
        return Footer.read(readable, lengthBuffer);
    }

    @Override
    public IndexRangeId id() {
        return id;
    }

    private Leaps leaps = null;
    private TLongObjectHashMap<Leaps> leapsCache;
    private Footer footer = null;

    @Override
    public ReadIndex reader(int bufferSize) throws Exception {
        hideABone.acquire();
        if (disposed.get()) {
            hideABone.release();
            return null;
        }

        try {
            IReadable readableIndex = index.reader(null, index.length(), bufferSize);

            if (leaps == null) {
                long indexLength = readableIndex.length();
                if (indexLength < 4) {
                    System.out.println("WTF:" + indexLength);
                }
                readableIndex.seek(indexLength - 4);
                int footerLength = UIO.readInt(readableIndex, "length", lengthBuffer);
                readableIndex.seek(indexLength - (footerLength + 1 + 4));
                int leapLength = UIO.readInt(readableIndex, "length", lengthBuffer);

                readableIndex.seek(indexLength - (1 + leapLength + 1 + footerLength));

                int type = readableIndex.read();
                if (type != LEAP) {
                    throw new RuntimeException("Corruption! " + type + " expected " + LEAP);
                }
                leaps = Leaps.read(readableIndex, lengthBuffer);
                leapsCache = new TLongObjectHashMap<>(footer.leapCount);

            }
            return new ReadLeapsAndBoundsIndex(disposed, hideABone, new ActiveScan(leaps, leapsCache, footer, readableIndex, lengthBuffer));
        } catch (IOException | RuntimeException x) {
            throw x;
        } finally {
            hideABone.release();
        }
    }

    @Override
    public void destroy() throws IOException {
        destroy.submit(() -> {

            hideABone.acquire(numBonesHidden);
            disposed.set(true);
            try {
                index.close();
                new File(index.getFileName()).delete();
            } finally {
                hideABone.release(numBonesHidden);
            }
            return null;
        });

    }

    @Override
    public void close() throws Exception {
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
    public String toString() {
        return "LeapsAndBoundsIndex{" + "index=" + index + '}';
    }

}
