package com.jivesoftware.os.lab.guts;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.IReadable;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;

/**
 *
 * @author jonathan.colt
 */
public class Leaps {

    private final int index;
    final byte[] lastKey;
    final long[] fps;
    final byte[][] keys;
    final LongBuffer startOfEntryBuffer;
    //final long[] startOfEntryIndex;

    public Leaps(int index, byte[] lastKey, long[] fpIndex, byte[][] keys, LongBuffer startOfEntryBuffer) {
        this.index = index;
        this.lastKey = lastKey;
        Preconditions.checkArgument(fpIndex.length == keys.length, "fpIndex and keys misalignment, %s != %s", fpIndex.length, keys.length);
        this.fps = fpIndex;
        this.keys = keys;
        this.startOfEntryBuffer = startOfEntryBuffer;
    }

    void write(IAppendOnly writeable, byte[] lengthBuffer) throws IOException {
        int entryLength = 4 + 4 + 4 + lastKey.length + 4 + (startOfEntryBuffer.limit() * 8) + 4;
        for (int i = 0; i < fps.length; i++) {
            entryLength += 8 + 4 + keys[i].length;
        }
        entryLength += 4;
        UIO.writeInt(writeable, entryLength, "entryLength", lengthBuffer);
        UIO.writeInt(writeable, index, "index", lengthBuffer);
        UIO.writeInt(writeable, lastKey.length, "lastKeyLength", lengthBuffer);
        UIO.write(writeable, lastKey, "lastKey");
        UIO.writeInt(writeable, fps.length, "fpIndexLength", lengthBuffer);
        for (int i = 0; i < fps.length; i++) {
            UIO.writeLong(writeable, fps[i], "fpIndex");
            UIO.writeByteArray(writeable, keys[i], "key", lengthBuffer);
        }
        int startOfEntryLength = startOfEntryBuffer.limit();
        UIO.writeInt(writeable, startOfEntryLength, "startOfEntryLength", lengthBuffer);
        for (int i = 0; i < startOfEntryLength; i++) {
            UIO.writeLong(writeable, startOfEntryBuffer.get(i), "entry");
        }
        UIO.writeInt(writeable, entryLength, "entryLength", lengthBuffer);
    }

    static Leaps read(IReadable readable, byte[] lengthBuffer) throws IOException {
        int entryLength = UIO.readInt(readable, "entryLength", lengthBuffer);
        int index = UIO.readInt(readable, "index", lengthBuffer);
        int lastKeyLength = UIO.readInt(readable, "lastKeyLength", lengthBuffer);
        byte[] lastKey = new byte[lastKeyLength];
        UIO.read(readable, lastKey);
        int fpIndexLength = UIO.readInt(readable, "fpIndexLength", lengthBuffer);
        long[] fpIndex = new long[fpIndexLength];
        byte[][] keys = new byte[fpIndexLength][];
        for (int i = 0; i < fpIndexLength; i++) {
            fpIndex[i] = UIO.readLong(readable, "fpIndex", lengthBuffer);
            keys[i] = UIO.readByteArray(readable, "keyLength", lengthBuffer);
        }
        int startOfEntryLength = UIO.readInt(readable, "startOfEntryLength", lengthBuffer);
        ByteBuffer startOfEntryByteBuffer = readable.slice(startOfEntryLength * 8);
        if (UIO.readInt(readable, "entryLength", lengthBuffer) != entryLength) {
            throw new RuntimeException("Encountered length corruption. ");
        }
        startOfEntryByteBuffer.position(0);
        return new Leaps(index, lastKey, fpIndex, keys, startOfEntryByteBuffer.asLongBuffer());
    }

}
