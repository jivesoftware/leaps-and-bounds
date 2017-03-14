package com.jivesoftware.os.lab.guts;

import com.jivesoftware.os.lab.api.FormatTransformer;
import com.jivesoftware.os.lab.api.rawhide.Rawhide;
import com.jivesoftware.os.lab.guts.api.RawEntryStream;
import com.jivesoftware.os.lab.guts.api.ReadIndex;
import com.jivesoftware.os.lab.guts.api.Scanner;
import com.jivesoftware.os.lab.io.BolBuffer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;

/**
 * Created by jonathan.colt on 3/14/17.
 */
public class PointInterleave implements Scanner, RawEntryStream {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    private final Rawhide rawhide;
    private FormatTransformer nextReadKeyFormatTransformer;
    private FormatTransformer nextReadValueFormatTransformer;
    private boolean once;
    private BolBuffer nextRawEntry;


    public PointInterleave(ReadIndex[] indexs, byte[] key, Rawhide rawhide, boolean hashIndexEnabled) throws Exception {
        this.rawhide = rawhide;
        BolBuffer entryBuffer = new BolBuffer();
        BolBuffer entryKeyBuffer = new BolBuffer();
        for (int i = 0; i < indexs.length; i++) {
            Scanner scanner = null;
            try {
                scanner = indexs[i].pointScan(new ActiveScan(hashIndexEnabled), key, entryBuffer, entryKeyBuffer);
                if (scanner != null) {
                    scanner.next(this);
                    scanner.close();
                }
            } catch (Throwable t) {
                if (scanner != null) {
                    scanner.close();
                }
                throw t;
            }
        }
    }

    @Override
    public Next next(RawEntryStream stream) throws Exception {
        if (once) {
            return Next.stopped;
        }
        stream.stream(nextReadKeyFormatTransformer, nextReadValueFormatTransformer, nextRawEntry);
        once = true;
        return Next.more;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public boolean stream(FormatTransformer readKeyFormatTransformer, FormatTransformer readValueFormatTransformer, BolBuffer rawEntry) throws Exception {
        if (nextRawEntry != null) {
            long leftTimestamp = rawhide.timestamp(nextReadKeyFormatTransformer, nextReadValueFormatTransformer, nextRawEntry);
            long rightTimestamp = rawhide.timestamp(readKeyFormatTransformer, readValueFormatTransformer, rawEntry);
            if (leftTimestamp > rightTimestamp) {
                return true;
            } else if (leftTimestamp == rightTimestamp) {
                long leftVersion = rawhide.version(nextReadKeyFormatTransformer, nextReadValueFormatTransformer, nextRawEntry);
                long rightVersion = rawhide.version(readKeyFormatTransformer, readValueFormatTransformer, rawEntry);
                if (leftVersion > rightVersion) {
                    return true;
                }
            }
        }
        nextRawEntry = rawEntry;
        nextReadKeyFormatTransformer = readKeyFormatTransformer;
        nextReadValueFormatTransformer = readValueFormatTransformer;
        return true;
    }
}
