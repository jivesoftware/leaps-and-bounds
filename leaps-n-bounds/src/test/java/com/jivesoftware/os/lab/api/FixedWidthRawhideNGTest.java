package com.jivesoftware.os.lab.api;

import com.jivesoftware.os.lab.io.api.UIO;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan.colt
 */
public class FixedWidthRawhideNGTest {

    @Test
    public void bbCompareTest() {
        FixedWidthRawhide rawhide = new FixedWidthRawhide(8, 8);
        Assert.assertEquals(rawhide.compareBB(UIO.longBytes(1), 0, 8, UIO.longBytes(1), 0, 8), 0);
        Assert.assertEquals(rawhide.compareBB(UIO.longBytes(10), 0, 8, UIO.longBytes(20), 0, 8), -10);
        Assert.assertEquals(rawhide.compareBB(UIO.longBytes(0), 0, 8, UIO.longBytes(1), 0, 8), -1);
        Assert.assertEquals(rawhide.compareBB(UIO.longBytes(1), 0, 8, UIO.longBytes(0), 0, 8), 1);

        Long[] sort = new Long[]{new Long(9), new Long(5), new Long(6), new Long(3), new Long(4), new Long(5), new Long(1), new Long(2), new Long(9)};
        Arrays.sort(sort, new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return rawhide.compareBB(UIO.longBytes(o1), 0, 8, UIO.longBytes(o2), 0, 8);
            }
        });

        long[] sorted = new long[]{1, 2, 3, 4, 5, 5, 6, 9, 9};
        for (int i = 0; i < sorted.length; i++) {
            Assert.assertEquals((long) sort[i], sorted[i]);
        }
    }

    @Test
    public void compareKeyTest() {
        FixedWidthRawhide rawhide = new FixedWidthRawhide(8, 8);
        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP, ByteBuffer.wrap(UIO.longBytes(1)), ByteBuffer.wrap(UIO
            .longBytes(1))), 0);
        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP, ByteBuffer.wrap(UIO.longBytes(0)), ByteBuffer.wrap(UIO
            .longBytes(1))), -1);
        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP, ByteBuffer.wrap(UIO.longBytes(1)), ByteBuffer.wrap(UIO
            .longBytes(0))), 1);

        Long[] sort = new Long[]{new Long(9), new Long(5), new Long(6), new Long(3), new Long(4), new Long(5), new Long(1), new Long(2), new Long(9)};
        Arrays.sort(sort, new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP, ByteBuffer.wrap(UIO.longBytes(o1)), ByteBuffer.wrap(UIO
                    .longBytes(o2)));
            }
        });

        long[] sorted = new long[]{1, 2, 3, 4, 5, 5, 6, 9, 9};
        for (int i = 0; i < sorted.length; i++) {
            Assert.assertEquals((long) sort[i], sorted[i]);
        }
    }

    @Test
    public void compareKey2Test() {
        FixedWidthRawhide rawhide = new FixedWidthRawhide(8, 8);
        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP, ByteBuffer.wrap(UIO.longBytes(1)),
            FormatTransformer.NO_OP, FormatTransformer.NO_OP, ByteBuffer.wrap(UIO.longBytes(1))), 0);
        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP, ByteBuffer.wrap(UIO.longBytes(0)),
            FormatTransformer.NO_OP, FormatTransformer.NO_OP, ByteBuffer.wrap(UIO.longBytes(1))), -1);
        Assert.assertEquals(rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP, ByteBuffer.wrap(UIO.longBytes(1)),
            FormatTransformer.NO_OP, FormatTransformer.NO_OP, ByteBuffer.wrap(UIO.longBytes(0))), 1);

        Long[] sort = new Long[]{new Long(9), new Long(5), new Long(6), new Long(3), new Long(4), new Long(5), new Long(1), new Long(2), new Long(9)};
        Arrays.sort(sort, new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return rawhide.compareKey(FormatTransformer.NO_OP, FormatTransformer.NO_OP, ByteBuffer.wrap(UIO.longBytes(o1)),
                    FormatTransformer.NO_OP, FormatTransformer.NO_OP, ByteBuffer.wrap(UIO.longBytes(o2)));
            }
        });

        long[] sorted = new long[]{1, 2, 3, 4, 5, 5, 6, 9, 9};
        for (int i = 0; i < sorted.length; i++) {
            Assert.assertEquals((long) sort[i], sorted[i]);
        }
    }
    
}
