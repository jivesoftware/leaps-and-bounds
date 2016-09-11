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
    final ByteBuffer lastKey;
    final long[] fps;
    final ByteBuffer[] keys;
    final StartOfEntry startOfEntry;
    //final long[] startOfEntryIndex;

    public interface StartOfEntry {

        LongBuffer get(IReadable readable) throws IOException;
    }

    public Leaps(int index, ByteBuffer lastKey, long[] fpIndex, ByteBuffer[] keys, StartOfEntry startOfEntry) {
        this.index = index;
        this.lastKey = lastKey;
        Preconditions.checkArgument(fpIndex.length == keys.length, "fpIndex and keys misalignment, %s != %s", fpIndex.length, keys.length);
        this.fps = fpIndex;
        this.keys = keys;
        this.startOfEntry = startOfEntry;
    }

    void write(FormatTransformer keyFormatTransformer, IAppendOnly writeable) throws Exception {
        ByteBuffer writeLastKey = keyFormatTransformer.transform(lastKey);
        ByteBuffer[] writeKeys = keyFormatTransformer.transform(keys);

        LongBuffer startOfEntryBuffer = startOfEntry.get(null);
        int entryLength = 4 + 4 + 4 + writeLastKey.capacity() + 4 + (startOfEntryBuffer.limit() * 8) + 4;
        for (int i = 0; i < fps.length; i++) {
            entryLength += 8 + 4 + writeKeys[i].capacity();
        }
        entryLength += 4;
        UIO.writeInt(writeable, entryLength, "entryLength");
        UIO.writeInt(writeable, index, "index");
        UIO.writeInt(writeable, writeLastKey.capacity(), "lastKeyLength");

        writeLastKey.clear();
        byte[] array = new byte[writeLastKey.capacity()];
        writeLastKey.get(array);
        writeable.append(array, 0, array.length);

        UIO.writeInt(writeable, fps.length, "fpIndexLength");
        for (int i = 0; i < fps.length; i++) {
            writeable.appendLong(fps[i]);
            UIO.writeByteArray(writeable, writeKeys[i], "key");
        }
        int startOfEntryLength = startOfEntryBuffer.limit();
        UIO.writeInt(writeable, startOfEntryLength, "startOfEntryLength");
        for (int i = 0; i < startOfEntryLength; i++) {
            writeable.appendLong(startOfEntryBuffer.get(i));
        }
        UIO.writeInt(writeable, entryLength, "entryLength");
    }

    static Leaps read(FormatTransformer keyFormatTransformer, IReadable readable) throws Exception {
        int entryLength = UIO.readInt(readable, "entryLength");
        int index = UIO.readInt(readable, "index");
        int lastKeyLength = UIO.readInt(readable, "lastKeyLength");
        byte[] lastKey = new byte[lastKeyLength];
        UIO.read(readable, lastKey);
        int fpIndexLength = UIO.readInt(readable, "fpIndexLength");
        long[] fpIndex = new long[fpIndexLength];
        ByteBuffer[] keys = new ByteBuffer[fpIndexLength];
        for (int i = 0; i < fpIndexLength; i++) {
            fpIndex[i] = UIO.readLong(readable, "fpIndex");
            keys[i] = ByteBuffer.wrap(UIO.readByteArray(readable, "keyLength"));
        }
        int startOfEntryLength = UIO.readInt(readable, "startOfEntryLength");
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
            readable.read(startOfEntryBytes, 0, startOfEntryBytes.length);
            LongBuffer startOfEntryBuffer = ByteBuffer.wrap(startOfEntryBytes).asLongBuffer();
            startOfEntry = readable1 -> startOfEntryBuffer;
        }
        if (UIO.readInt(readable, "entryLength") != entryLength) {
            throw new RuntimeException("Encountered length corruption. ");
        }
        return new Leaps(index, keyFormatTransformer.transform(ByteBuffer.wrap(lastKey)), fpIndex, keyFormatTransformer.transform(keys), startOfEntry);
    }

}
