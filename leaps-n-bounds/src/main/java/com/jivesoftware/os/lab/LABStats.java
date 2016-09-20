package com.jivesoftware.os.lab;

import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import com.jivesoftware.os.mlogger.core.ValueType;
import java.util.concurrent.atomic.LongAdder;

/**
 *
 * @author jonathan.colt
 */
public class LABStats {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public final LongAdder open = new LongAdder();

    public final LongAdder append = new LongAdder();
    public final LongAdder journaledAppend = new LongAdder();

    public final LongAdder gets = new LongAdder();
    public final LongAdder rangeScan = new LongAdder();
    public final LongAdder multiRangeScan = new LongAdder();
    public final LongAdder rowScan = new LongAdder();

    public final LongAdder commit = new LongAdder();
    public final LongAdder fsyncedCommit = new LongAdder();

    public final LongAdder compacts = new LongAdder();

    public final LongAdder closed = new LongAdder();

    public final LongAdder allocationed = new LongAdder();
    public final LongAdder freed = new LongAdder();
    public final LongAdder gc = new LongAdder();

    public final LongAdder bytesWrittenToWAL = new LongAdder();
    public final LongAdder bytesWrittenAsIndex = new LongAdder();
    public final LongAdder bytesWrittenAsSplit = new LongAdder();
    public final LongAdder bytesWrittenAsMerge = new LongAdder();

    public void refreshJMX() {
        LOG.set(ValueType.VALUE, "open", open.longValue());
        LOG.set(ValueType.VALUE, "append", append.longValue());
        LOG.set(ValueType.VALUE, "journaledAppend", journaledAppend.longValue());
        LOG.set(ValueType.VALUE, "gets", gets.longValue());
        LOG.set(ValueType.VALUE, "rangeScan", rangeScan.longValue());
        LOG.set(ValueType.VALUE, "multiRangeScan", multiRangeScan.longValue());
        LOG.set(ValueType.VALUE, "rowScan", rowScan.longValue());
        LOG.set(ValueType.VALUE, "commit", commit.longValue());
        LOG.set(ValueType.VALUE, "fsyncedCommit", fsyncedCommit.longValue());
        LOG.set(ValueType.VALUE, "compacts", compacts.longValue());
        LOG.set(ValueType.VALUE, "closed", closed.longValue());
        LOG.set(ValueType.VALUE, "allocationed", allocationed.longValue());
        LOG.set(ValueType.VALUE, "freed", freed.longValue());
        LOG.set(ValueType.VALUE, "gc", gc.longValue());

        LOG.set(ValueType.VALUE, "bytesWrittenToWAL", bytesWrittenToWAL.longValue());
        LOG.set(ValueType.VALUE, "bytesWrittenAsIndex", bytesWrittenAsIndex.longValue());
        LOG.set(ValueType.VALUE, "bytesWrittenAsSplit", bytesWrittenAsSplit.longValue());
        LOG.set(ValueType.VALUE, "bytesWrittenAsMerge", bytesWrittenAsMerge.longValue());

    }
}
