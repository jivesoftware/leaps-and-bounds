package com.jivesoftware.os.lab.guts.allocators;

import com.jivesoftware.os.lab.BolBuffer;
import com.jivesoftware.os.lab.LABConcurrentSkipListMap;
import com.jivesoftware.os.lab.StripingBolBufferLocks;
import com.jivesoftware.os.lab.api.FixedWidthRawhide;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.guts.IndexUtil;
import com.jivesoftware.os.lab.guts.LABIndex;
import com.jivesoftware.os.lab.guts.api.Scanner;
import com.jivesoftware.os.lab.io.api.UIO;
import java.nio.ByteBuffer;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class LABConcurrentSkipListMapNGTest {

    @Test
    public void batTest() throws Exception {

        LABAppendOnlyAllocator allocator = new LABAppendOnlyAllocator(30);
        LABIndexableMemory labIndexableMemory = new LABIndexableMemory("test", allocator);
        FixedWidthRawhide rawhide = new FixedWidthRawhide(8, 8);

        LABConcurrentSkipListMap map = new LABConcurrentSkipListMap(new LABConcurrentSkipListMemory(rawhide, labIndexableMemory),
            new StripingBolBufferLocks(1024));

        for (int i = 0; i < 100; i++) {

            BolBuffer key = new BolBuffer(UIO.longBytes(i));
            BolBuffer value = new BolBuffer(UIO.longBytes(i));
            map.compute(key, value, new LABIndex.Compute() {
                @Override
                public BolBuffer apply(BolBuffer existing) {
                    return value;
                }
            });
        }
        System.out.println("Count:" + map.size());
        System.out.println("first:" + UIO.bytesLong(map.firstKey()));
        System.out.println("last:" + UIO.bytesLong(map.lastKey()));

        Scanner scanner = map.scanner(null, null);
        while (scanner.next((FormatTransformer readKeyFormatTransformer, FormatTransformer readValueFormatTransformer, ByteBuffer rawEntry) -> {
            System.out.println("Keys:" + UIO.bytesLong(IndexUtil.toByteArray(rawEntry)));
            return true;
        }) == Scanner.Next.more) {
        }

    }

}
