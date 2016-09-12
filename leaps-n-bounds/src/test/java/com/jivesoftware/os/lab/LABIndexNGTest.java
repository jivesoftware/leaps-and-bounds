package com.jivesoftware.os.lab;

import com.jivesoftware.os.lab.guts.LABCSLMIndex;
import com.jivesoftware.os.lab.guts.LABIndex;
import com.jivesoftware.os.lab.guts.allocators.LABAppendOnlyAllocator;
import com.jivesoftware.os.lab.guts.allocators.LABConcurrentSkipListMemory;
import com.jivesoftware.os.lab.guts.allocators.LABIndexableMemory;
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
        LABIndex map = new LABCSLMIndex(LABRawhide.SINGLETON, new StripingBolBufferLocks(1024));
        testLABIndex(map);

    }

    @Test
    public void testGet() throws Exception {
        LABIndex map = new LABConcurrentSkipListMap(
            new LABConcurrentSkipListMemory(
                LABRawhide.SINGLETON, new LABIndexableMemory("bla", new LABAppendOnlyAllocator(null)
                )
            ),
            new StripingBolBufferLocks(1024)
        );

        testLABIndex(map);

    }

    private void testLABIndex(LABIndex map) throws Exception {
        BolBuffer key = new BolBuffer(UIO.longBytes(8));
        BolBuffer value1 = new BolBuffer(UIO.longBytes(10));

        map.compute(key, new BolBuffer(), new LABIndex.Compute() {
            @Override
            public BolBuffer apply(BolBuffer existing) {
                if (existing == null) {
                    return value1;
                } else if (UIO.bytesLong(existing.copy()) > UIO.bytesLong(value1.copy())) {
                    return existing;
                } else {
                    return value1;
                }
            }
        });
        BolBuffer got = map.get(key, new BolBuffer());
        Assert.assertEquals(UIO.bytesLong(got.copy()), 10L);

        BolBuffer value2 = new BolBuffer(UIO.longBytes(21));
        map.compute(key, new BolBuffer(), new LABIndex.Compute() {
            @Override
            public BolBuffer apply(BolBuffer existing) {
                if (existing == null) {
                    return value2;
                } else if (UIO.bytesLong(existing.copy()) > UIO.bytesLong(value2.copy())) {
                    return existing;
                } else {
                    return value2;
                }
            }
        });
        got = map.get(key, new BolBuffer());
        Assert.assertEquals(UIO.bytesLong(got.copy()), 21L);

        BolBuffer value3 = new BolBuffer(UIO.longBytes(10));

        map.compute(key, new BolBuffer(), new LABIndex.Compute() {
            @Override
            public BolBuffer apply(BolBuffer existing) {
                if (existing == null) {
                    return value3;
                } else if (UIO.bytesLong(existing.copy()) > UIO.bytesLong(value3.copy())) {
                    return existing;
                } else {
                    return value3;
                }
            }
        });

        got = map.get(key, new BolBuffer());
        Assert.assertEquals(UIO.bytesLong(got.copy()), 21L);
    }

}
