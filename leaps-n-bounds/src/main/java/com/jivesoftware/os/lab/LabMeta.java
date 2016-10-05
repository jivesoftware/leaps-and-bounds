package com.jivesoftware.os.lab;

import com.jivesoftware.os.jive.utils.collections.bah.ConcurrentBAHash;
import com.jivesoftware.os.lab.guts.AppendOnlyFile;
import com.jivesoftware.os.lab.guts.ReadOnlyFile;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.IPointerReadable;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.File;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author jonathan.colt
 */
public class LabMeta {

    private final File metaRoot;
    private final AtomicReference<Meta> meta = new AtomicReference<>();

    public LabMeta(LABStats stats, File metaRoot) throws Exception {
        this.metaRoot = metaRoot;
        File activeMeta = new File(metaRoot, "active.meta");

        if (!activeMeta.exists()) {
            activeMeta.getParentFile().mkdirs();
        }
        Meta m = new Meta(activeMeta);
        if (activeMeta.exists()) {
            m.load();
        }

    }

    static class Meta {

        private final ReadOnlyFile readOnlyFile;
        private final AppendOnlyFile appendOnlyFile;
        private final IAppendOnly appender;
        private final ConcurrentBAHash<byte[]> keyOffsetCache;

        public Meta(File metaFile) throws Exception {

            this.appendOnlyFile = new AppendOnlyFile(metaFile);
            this.readOnlyFile = new ReadOnlyFile(metaFile);
            this.appender = appendOnlyFile.appender();
            this.keyOffsetCache = new ConcurrentBAHash<>(16, true, 1024);

        }

        public int load() throws Exception {
            int collisions = 0;
            IPointerReadable readable = readOnlyFile.pointerReadable(-1);
            long o = 0;
            while (o < readable.length()) {
                int keyLength = readable.readInt(o);
                o += 4;
                byte[] key = new byte[keyLength];
                readable.read(o, key, 0, keyLength);
                o += keyLength;
                byte[] got = keyOffsetCache.get(key);
                if (got != null) {
                    collisions++;
                }
                keyOffsetCache.put(key, UIO.longBytes(o));
                int valueLength = readable.readInt(o);
                o += 4;
                o += valueLength;
            }
            return collisions;
        }

        public void copyTo(Meta to) throws Exception {
            IPointerReadable pointerReadable = readOnlyFile.pointerReadable(-1);
            BolBuffer valueBolBuffer = new BolBuffer();
            keyOffsetCache.stream((key, valueOffset) -> {
                long offset = UIO.bytesLong(valueOffset);
                int length = pointerReadable.readInt(offset);
                pointerReadable.sliceIntoBuffer(offset, length, valueBolBuffer);
                to.append(key, valueOffset);
                return true;
            });
        }

        public BolBuffer get(byte[] key, BolBuffer valueBolBuffer) throws Exception {
            byte[] offsetBytes = keyOffsetCache.get(key, 0, key.length);
            if (offsetBytes == null) {
                return null;
            }
            long offset = UIO.bytesLong(offsetBytes);
            IPointerReadable pointerReadable = readOnlyFile.pointerReadable(-1);
            int length = pointerReadable.readInt(offset);
            pointerReadable.sliceIntoBuffer(offset + 4, length, valueBolBuffer);
            return valueBolBuffer;
        }

        synchronized public void append(byte[] key, byte[] value) throws Exception {
            appender.appendInt(key.length);
            appender.append(key, 0, key.length);
            long filePointer = appender.getFilePointer();
            appender.appendInt(value.length);
            appender.appendInt(key.length);

            keyOffsetCache.put(key, UIO.longBytes(filePointer));
        }
    }
}
