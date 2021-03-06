package com.jivesoftware.os.lab;

import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.rawhide.LABRawhide;
import com.jivesoftware.os.lab.guts.LABCSLMIndex;
import com.jivesoftware.os.lab.guts.LABIndex;
import com.jivesoftware.os.lab.guts.StripingBolBufferLocks;
import com.jivesoftware.os.lab.guts.allocators.LABAppendOnlyAllocator;
import com.jivesoftware.os.lab.guts.allocators.LABConcurrentSkipListMap;
import com.jivesoftware.os.lab.guts.allocators.LABConcurrentSkipListMemory;
import com.jivesoftware.os.lab.guts.allocators.LABIndexableMemory;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.lab.io.api.UIO;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LABIndexNGTest {

    @Test
    public void testCSLGet() throws Exception {
        LABIndex<BolBuffer, BolBuffer> map = new LABCSLMIndex(LABRawhide.SINGLETON, new StripingBolBufferLocks(1024));
        testLABIndex(map);

    }

    @Test
    public void testGet() throws Exception {
        LABIndex<BolBuffer, BolBuffer> map = new LABConcurrentSkipListMap(new LABStats(),
            new LABConcurrentSkipListMemory(
                LABRawhide.SINGLETON, new LABIndexableMemory( new LABAppendOnlyAllocator("test", 2))
            ),
            new StripingBolBufferLocks(1024)
        );

        testLABIndex(map);

    }

    private void testLABIndex(LABIndex<BolBuffer, BolBuffer> map) throws Exception {
        BolBuffer key = new BolBuffer(UIO.longBytes(8));
        BolBuffer value1 = new BolBuffer(UIO.longBytes(10));

        map.compute(FormatTransformer.NO_OP, FormatTransformer.NO_OP, new BolBuffer(), key, new BolBuffer(),
            (t1, t2, b, existing) -> {
                if (existing == null) {
                    return value1;
                } else if (UIO.bytesLong(existing.copy()) > UIO.bytesLong(value1.copy())) {
                    return existing;
                } else {
                    return value1;
                }
            },
            (added, reused) -> {
            }
        );
        BolBuffer got = map.get(key, new BolBuffer());
        Assert.assertEquals(UIO.bytesLong(got.copy()), 10L);

        BolBuffer value2 = new BolBuffer(UIO.longBytes(21));
        map.compute(FormatTransformer.NO_OP, FormatTransformer.NO_OP, new BolBuffer(),
            key,
            new BolBuffer(), (t1, t2, b, existing) -> {
                if (existing == null) {
                    return value2;
                } else if (UIO.bytesLong(existing.copy()) > UIO.bytesLong(value2.copy())) {
                    return existing;
                } else {
                    return value2;
                }
            },
            (added, reused) -> {
            }
        );
        got = map.get(key, new BolBuffer());
        Assert.assertEquals(UIO.bytesLong(got.copy()), 21L);

        BolBuffer value3 = new BolBuffer(UIO.longBytes(10));

        map.compute(FormatTransformer.NO_OP,
            FormatTransformer.NO_OP,
            new BolBuffer(),
            key,
            new BolBuffer(),
            (t1, t2, b, existing) -> {
                if (existing == null) {
                    return value3;
                } else if (UIO.bytesLong(existing.copy()) > UIO.bytesLong(value3.copy())) {
                    return existing;
                } else {
                    return value3;
                }
            },
            (added, reused) -> {
            }
        );

        got = map.get(key, new BolBuffer());
        Assert.assertEquals(UIO.bytesLong(got.copy()), 21L);
    }

}
