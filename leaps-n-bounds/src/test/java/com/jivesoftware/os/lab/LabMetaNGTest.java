package com.jivesoftware.os.lab;

import com.google.common.io.Files;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.api.UIO;
import java.io.File;
import java.util.HashSet;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LabMetaNGTest {

    @Test
    public void testGet() throws Exception {
        File tmpDir = Files.createTempDir();

        LabMeta meta = new LabMeta(tmpDir);

        for (int i = 0; i < 10; i++) {
            meta.append(UIO.longBytes(i), UIO.longBytes(i));
        }

        for (int i = 0; i < 10; i++) {
            byte[] value = meta.get(UIO.longBytes(i), BolBuffer::copy);
            Assert.assertTrue(UIO.bytesLong(value) == i);
        }

        for (int i = 0; i < 10; i++) {
            meta.append(UIO.longBytes(i), UIO.longBytes(i * 2));
        }

        for (int i = 0; i < 10; i++) {
            byte[] value = meta.get(UIO.longBytes(i), BolBuffer::copy);
            Assert.assertTrue(UIO.bytesLong(value) == i * 2);
        }

        meta.close();

        meta = new LabMeta(tmpDir);
        Set<Long> keys = new HashSet<>();
        meta.metaKeys((byte[] metaKey) -> {
            keys.add(UIO.bytesLong(metaKey));
            return true;
        });

        Assert.assertTrue(keys.size() == 10);

        for (int i = 0; i < 10; i++) {
            meta.append(UIO.longBytes(i), UIO.longBytes(i * 3));
        }

        for (int i = 0; i < 10; i++) {
            byte[] value = meta.get(UIO.longBytes(i), BolBuffer::copy);
            Assert.assertTrue(UIO.bytesLong(value) == i * 3);
        }

        meta.metaKeys((byte[] metaKey) -> {
            keys.add(UIO.bytesLong(metaKey));
            return true;
        });

        Assert.assertTrue(keys.size() == 10);

    }

}
