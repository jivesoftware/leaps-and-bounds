package com.jivesoftware.os.lab;

import com.jivesoftware.os.jive.utils.ordered.id.ConstantWriterIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProvider;
import com.jivesoftware.os.jive.utils.ordered.id.OrderIdProviderImpl;
import com.jivesoftware.os.lab.BolBuffer;
import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.guts.CompactableIndexes;
import com.jivesoftware.os.lab.guts.InterleaveStream;
import com.jivesoftware.os.lab.guts.PointGetRaw;
import com.jivesoftware.os.lab.guts.SimpleRawhide;
import com.jivesoftware.os.lab.guts.api.GetRaw;
import com.jivesoftware.os.lab.guts.api.RawAppendableIndex;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.Scanner;
import com.jivesoftware.os.lab.io.api.UIO;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import org.testng.Assert;

import static com.jivesoftware.os.lab.guts.SimpleRawhide.rawEntry;
import static com.jivesoftware.os.lab.guts.SimpleRawhide.key;
import static com.jivesoftware.os.lab.guts.SimpleRawhide.value;
import static com.jivesoftware.os.lab.guts.SimpleRawhide.key;
import static com.jivesoftware.os.lab.guts.SimpleRawhide.value;

/**
 * @author jonathan.colt
 */
public class TestUtils {

    private static final OrderIdProvider timeProvider = new OrderIdProviderImpl(new ConstantWriterIdProvider(1));

    static public long append(Random rand,
        RawAppendableIndex index,
        long start,
        int step,
        int count,
        ConcurrentSkipListMap<byte[], byte[]> desired,
        BolBuffer keyBuffer) throws Exception {

        long[] lastKey = new long[1];
        index.append((stream) -> {
            long k = start;
            for (int i = 0; i < count; i++) {
                k += 1 + rand.nextInt(step);

                byte[] key = UIO.longBytes(k, new byte[8], 0);
                long time = timeProvider.nextId();

                long specialK = k;
                if (desired != null) {
                    desired.compute(key, (t, u) -> {
                        if (u == null) {
                            return rawEntry(specialK, time);
                        } else {
                            return value(u) > time ? u : rawEntry(specialK, time);
                        }
                    });
                }
                byte[] rawEntry = rawEntry(k, time);
                if (!stream.stream(FormatTransformer.NO_OP, FormatTransformer.NO_OP, new BolBuffer(rawEntry))) {
                    break;
                }
            }
            lastKey[0] = k;
            return true;
        }, keyBuffer);
        return lastKey[0];
    }

    static public void assertions(CompactableIndexes indexes,
        int count, int step,
        ConcurrentSkipListMap<byte[], byte[]> desired) throws
        Exception {

        ArrayList<byte[]> keys = new ArrayList<>(desired.navigableKeySet());

        int[] index = new int[1];

        SimpleRawhide rawhide = new SimpleRawhide();
        indexes.tx(-1, null, null, (index1, fromKey, toKey, acquired, hydrateValues) -> {
            Scanner rowScan = new InterleaveStream(acquired, null, null, rawhide);
            try {
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                    System.out.println("scanned:" + UIO.bytesLong(keys.get(index[0])) + " " + key(rawEntry));
                    Assert.assertEquals(UIO.bytesLong(keys.get(index[0])), key(rawEntry));
                    index[0]++;
                    return true;
                };
                while (rowScan.next(stream) == Scanner.Next.more) {
                }
            } finally {
                rowScan.close();
            }
            System.out.println("rowScan PASSED");
            return true;
        }, true);

        indexes.tx(-1, null, null, (index1, fromKey, toKey, acquired, hydrateValues) -> {
            for (int i = 0; i < count * step; i++) {
                long k = i;
                GetRaw getRaw = new PointGetRaw(acquired);
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                    byte[] expectedFP = desired.get(UIO.longBytes(key(rawEntry), new byte[8], 0));
                    if (expectedFP == null) {
                        Assert.assertTrue(expectedFP == null && value(rawEntry) == -1);
                    } else {
                        Assert.assertEquals(value(expectedFP), value(rawEntry));
                    }
                    return true;
                };

                while (getRaw.get(UIO.longBytes(k, new byte[8], 0), stream)) {
                }
            }
            System.out.println("gets PASSED");
            return true;
        }, true);

        indexes.tx(-1, null, null, (index1, fromKey, toKey, acquired, hydrateValues) -> {
            for (int i = 0; i < keys.size() - 3; i++) {
                int _i = i;

                int[] streamed = new int[1];
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                    if (value(rawEntry) > -1) {
                        System.out.println("Streamed:" + key(rawEntry));
                        streamed[0]++;
                    }
                    return true;
                };

                System.out.println("Asked:" + UIO.bytesLong(keys.get(_i)) + " to " + UIO.bytesLong(keys.get(_i + 3)));
                Scanner rangeScan = new InterleaveStream(acquired, keys.get(_i), keys.get(_i + 3), rawhide);
                try {
                    while (rangeScan.next(stream) == Scanner.Next.more) {
                    }
                } finally {
                    rangeScan.close();
                }
                Assert.assertEquals(3, streamed[0]);
            }

            System.out.println("rangeScan PASSED");
            return true;
        }, true);

        indexes.tx(-1, null, null, (index1, fromKey, toKey, acquired, hydrateValues) -> {
            for (int i = 0; i < keys.size() - 3; i++) {
                int _i = i;
                int[] streamed = new int[1];
                RawEntryStream stream = (readKeyFormatTransformer, readValueFormatTransformer, rawEntry) -> {
                    if (value(rawEntry) > -1) {
                        streamed[0]++;
                    }
                    return true;
                };
                Scanner rangeScan = new InterleaveStream(acquired, UIO.longBytes(UIO.bytesLong(keys.get(_i)) + 1, new byte[8], 0), keys.get(_i + 3),
                    rawhide);
                try {
                    while (rangeScan.next(stream) == Scanner.Next.more) {
                    }
                } finally {
                    rangeScan.close();
                }
                Assert.assertEquals(2, streamed[0]);

            }

            System.out.println("rangeScan2 PASSED");
            return true;
        }, true);
    }
}
