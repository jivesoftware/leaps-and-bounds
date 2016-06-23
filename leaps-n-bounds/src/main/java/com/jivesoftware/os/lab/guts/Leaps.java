package com.jivesoftware.os.lab.guts;

import com.google.common.base.Preconditions;
import com.jivesoftware.os.lab.api.FormatTransformer;
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
    final StartOfEntry startOfEntry;
    //final long[] startOfEntryIndex;

    public interface StartOfEntry {
        LongBuffer get(IReadable readable) throws IOException;
    }

    public Leaps(int index, byte[] lastKey, long[] fpIndex, byte[][] keys, StartOfEntry startOfEntry) {
        this.index = index;
        this.lastKey = lastKey;
        Preconditions.checkArgument(fpIndex.length == keys.length, "fpIndex and keys misalignment, %s != %s", fpIndex.length, keys.length);
        this.fps = fpIndex;
        this.keys = keys;
        this.startOfEntry = startOfEntry;
    }

    void write(FormatTransformer keyFormatTransformer, IAppendOnly writeable, byte[] lengthBuffer) throws Exception {
        byte[] writeLastKey = keyFormatTransformer.transform(lastKey);
        byte[][] writeKeys =  keyFormatTransformer.transform(keys);
        
        LongBuffer startOfEntryBuffer = startOfEntry.get(null);
        int entryLength = 4 + 4 + 4 + writeLastKey.length + 4 + (startOfEntryBuffer.limit() * 8) + 4;
        for (int i = 0; i < fps.length; i++) {
            entryLength += 8 + 4 + writeKeys[i].length;
        }
        entryLength += 4;
        UIO.writeInt(writeable, entryLength, "entryLength", lengthBuffer);
        UIO.writeInt(writeable, index, "index", lengthBuffer);
        UIO.writeInt(writeable, lastKey.length, "lastKeyLength", lengthBuffer);
        UIO.write(writeable, writeLastKey, "lastKey");
        UIO.writeInt(writeable, fps.length, "fpIndexLength", lengthBuffer);
        for (int i = 0; i < fps.length; i++) {
            UIO.writeLong(writeable, fps[i], "fpIndex");
            UIO.writeByteArray(writeable, writeKeys[i], "key", lengthBuffer);
        }
        int startOfEntryLength = startOfEntryBuffer.limit();
        UIO.writeInt(writeable, startOfEntryLength, "startOfEntryLength", lengthBuffer);
        for (int i = 0; i < startOfEntryLength; i++) {
            UIO.writeLong(writeable, startOfEntryBuffer.get(i), "entry");
        }
        UIO.writeInt(writeable, entryLength, "entryLength", lengthBuffer);
    }

    static Leaps read(FormatTransformer keyFormatTransformer, IReadable readable, byte[] lengthBuffer) throws Exception {
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
        int startOfEntryNumBytes = startOfEntryLength * 8;
        StartOfEntry startOfEntry;
        if (readable.canSlice(startOfEntryNumBytes)) {
            long startOfEntryFp = readable.getFilePointer();
            readable.seek(startOfEntryFp + startOfEntryNumBytes);
            startOfEntry = readable1 -> {
                readable1.seek(startOfEntryFp);
                return readable1.slice(startOfEntryNumBytes).asLongBuffer();
            };
        } else {
            byte[] startOfEntryBytes = new byte[startOfEntryNumBytes];
            readable.read(startOfEntryBytes);
            LongBuffer startOfEntryBuffer = ByteBuffer.wrap(startOfEntryBytes).asLongBuffer();
            startOfEntry = readable1 -> startOfEntryBuffer;
        }
        if (UIO.readInt(readable, "entryLength", lengthBuffer) != entryLength) {
            throw new RuntimeException("Encountered length corruption. ");
        }
        return new Leaps(index, keyFormatTransformer.transform(lastKey), fpIndex, keyFormatTransformer.transform(keys), startOfEntry);
    }

}
