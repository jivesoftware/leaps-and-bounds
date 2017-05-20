package com.jivesoftware.os.lab.guts;

import com.google.common.io.Files;
import com.jivesoftware.os.lab.io.PointerReadableByteBufferFile;
import com.jivesoftware.os.lab.io.api.IAppendOnly;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.File;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class AppendOnlyFileNGTest {

    @Test
    public void test() throws Exception {
        File f = new File(Files.createTempDir(), "indexFile.bin");
        AppendOnlyFile appendOnlyFile = new AppendOnlyFile(f);

        IAppendOnly appender = appendOnlyFile.appender();
        for (int i = 0; i < 1_000; i++) {
            appender.append(UIO.longsBytes(new long[]{i + 1, i + 2, i + 3}), 0, 3 * 8);
        }
        appendOnlyFile.flush(true);
        appendOnlyFile.close();

        ReadOnlyFile readOnlyFile = new ReadOnlyFile(f);
        PointerReadableByteBufferFile pointerReadable = readOnlyFile.pointerReadable(-1);
        long offset = 0;
        for (int i = 0; i < 1_000; i++) {
            long a = pointerReadable.readLong(offset);
            offset += 8;
            long b = pointerReadable.readLong(offset);
            offset += 8;
            long c = pointerReadable.readLong(offset);
            offset += 8;

            Assert.assertEquals(new long[]{i + 1, i + 2, i + 3}, new long[]{a, b, c});

        }

    }

}
