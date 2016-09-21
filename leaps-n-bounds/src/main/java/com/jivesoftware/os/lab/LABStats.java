package com.jivesoftware.os.lab;

import com.jivesoftware.os.lab.guts.LABSparseCircularMetricBuffer;
import com.jivesoftware.os.mlogger.core.MetricLogger;
import com.jivesoftware.os.mlogger.core.MetricLoggerFactory;
import java.util.concurrent.atomic.LongAdder;

/**
 *
 * @author jonathan.colt
 */
public class LABStats {

    private static final MetricLogger LOG = MetricLoggerFactory.getLogger();

    public final LongAdder open = new LongAdder();
    public final LongAdder closed = new LongAdder();

    public final LongAdder append = new LongAdder();
    public final LongAdder journaledAppend = new LongAdder();

    public final LongAdder gets = new LongAdder();
    public final LongAdder rangeScan = new LongAdder();
    public final LongAdder multiRangeScan = new LongAdder();
    public final LongAdder rowScan = new LongAdder();

    public final LongAdder commit = new LongAdder();
    public final LongAdder fsyncedCommit = new LongAdder();

    public final LongAdder merging = new LongAdder();
    public final LongAdder merged = new LongAdder();
    public final LongAdder spliting = new LongAdder();
    public final LongAdder splits = new LongAdder();
    
    public final LongAdder slabbed = new LongAdder();
    public final LongAdder allocationed = new LongAdder();
    public final LongAdder released = new LongAdder();
    public final LongAdder freed = new LongAdder();
    public final LongAdder gc = new LongAdder();

    public final LongAdder bytesWrittenToWAL = new LongAdder();
    public final LongAdder bytesWrittenAsIndex = new LongAdder();
    public final LongAdder bytesWrittenAsSplit = new LongAdder();
    public final LongAdder bytesWrittenAsMerge = new LongAdder();

    public final LABSparseCircularMetricBuffer mOpen = new LABSparseCircularMetricBuffer(180, 0, 10_000); // TODO expose?
    public final LABSparseCircularMetricBuffer mClosed = new LABSparseCircularMetricBuffer(180, 0, 10_000); // TODO expose?

    public final LABSparseCircularMetricBuffer mAppend = new LABSparseCircularMetricBuffer(180, 0, 10_000); // TODO expose?
    public final LABSparseCircularMetricBuffer mJournaledAppend = new LABSparseCircularMetricBuffer(360, 0, 10_000); // TODO expose?

    public final LABSparseCircularMetricBuffer mGets = new LABSparseCircularMetricBuffer(180, 0, 10_000); // TODO expose?
    public final LABSparseCircularMetricBuffer mRangeScan = new LABSparseCircularMetricBuffer(180, 0, 10_000); // TODO expose?
    public final LABSparseCircularMetricBuffer mMultiRangeScan = new LABSparseCircularMetricBuffer(180, 0, 10_000); // TODO expose?
    public final LABSparseCircularMetricBuffer mRowScan = new LABSparseCircularMetricBuffer(180, 0, 10_000); // TODO expose?

    public final LABSparseCircularMetricBuffer mCommit = new LABSparseCircularMetricBuffer(180, 0, 10_000); // TODO expose?
    public final LABSparseCircularMetricBuffer mFsyncedCommit = new LABSparseCircularMetricBuffer(180, 0, 10_000); // TODO expose?

    public final LABSparseCircularMetricBuffer mMerging = new LABSparseCircularMetricBuffer(180, 0, 10_000); // TODO expose?
    public final LABSparseCircularMetricBuffer mMerged = new LABSparseCircularMetricBuffer(180, 0, 10_000); // TODO expose?
    public final LABSparseCircularMetricBuffer mSplitings = new LABSparseCircularMetricBuffer(180, 0, 10_000); // TODO
    public final LABSparseCircularMetricBuffer mSplits = new LABSparseCircularMetricBuffer(180, 0, 10_000); // TODO

    public final LABSparseCircularMetricBuffer mSlabbed = new LABSparseCircularMetricBuffer(180, 0, 10_000); // TODO expose?
    public final LABSparseCircularMetricBuffer mAllocationed = new LABSparseCircularMetricBuffer(180, 0, 10_000); // TODO expose?
    public final LABSparseCircularMetricBuffer mReleased = new LABSparseCircularMetricBuffer(180, 0, 10_000); // TODO expose?
    public final LABSparseCircularMetricBuffer mFreed = new LABSparseCircularMetricBuffer(180, 0, 10_000); // TODO
    public final LABSparseCircularMetricBuffer mGC = new LABSparseCircularMetricBuffer(180, 0, 10_000); // TODO expose?

    public final LABSparseCircularMetricBuffer mBytesWrittenToWAL = new LABSparseCircularMetricBuffer(180, 0, 10_000); // TODO expose?
    public final LABSparseCircularMetricBuffer mBytesWrittenAsIndex = new LABSparseCircularMetricBuffer(180, 0, 10_000); // TODO expose?
    public final LABSparseCircularMetricBuffer mBytesWrittenAsSplit = new LABSparseCircularMetricBuffer(180, 0, 10_000); // TODO expose?
    public final LABSparseCircularMetricBuffer mBytesWrittenAsMerge = new LABSparseCircularMetricBuffer(180, 0, 10_000); // TODO expose?

    public void refresh() {
        long timestamp = System.currentTimeMillis();
        mOpen.add(timestamp, open);
        mClosed.add(timestamp, closed);

        mAppend.add(timestamp, append);
        mJournaledAppend.add(timestamp, journaledAppend);

        mGets.add(timestamp, gets);
        mRangeScan.add(timestamp, rangeScan);
        mMultiRangeScan.add(timestamp, multiRangeScan);
        mRowScan.add(timestamp, rowScan);

        mCommit.add(timestamp, commit);
        mFsyncedCommit.add(timestamp, fsyncedCommit);

        mMerging.add(timestamp, merging);
        mMerged.add(timestamp, merged);
        mSplitings.add(timestamp, spliting);
        mSplits.add(timestamp, splits);

        mSlabbed.add(timestamp, slabbed);
        mAllocationed.add(timestamp, allocationed);
        mReleased.add(timestamp, released);
        mFreed.add(timestamp, freed);
        mGC.add(timestamp, gc);

        mBytesWrittenToWAL.add(timestamp, bytesWrittenToWAL);
        mBytesWrittenAsIndex.add(timestamp, bytesWrittenAsIndex);
        mBytesWrittenAsSplit.add(timestamp, bytesWrittenAsSplit);
        mBytesWrittenAsMerge.add(timestamp, bytesWrittenAsMerge);
    }
}
