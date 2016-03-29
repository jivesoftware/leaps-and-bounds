package com.jivesoftware.os.lab.guts;

/**
 *
 * @author jonathan.colt
 */
public class TimestampAndVersion {

    public static final TimestampAndVersion NULL = new TimestampAndVersion(-1, -1);

    final long maxTimestamp;
    final long maxTimestampVersion;

    public TimestampAndVersion(long maxTimestamp, long maxTimestampVersion) {
        this.maxTimestamp = maxTimestamp;
        this.maxTimestampVersion = maxTimestampVersion;
    }

    public int compare(long otherMaxTimestamp, long otherMaxTimestampVersion) {
        int c = Long.compare(maxTimestamp, otherMaxTimestamp);
        if (c != 0) {
            return c;
        }
        return Long.compare(maxTimestampVersion, otherMaxTimestampVersion);
    }

    @Override
    public String toString() {
        return "TimestampAndVersion{" + "maxTimestamp=" + maxTimestamp + ", maxTimestampVersion=" + maxTimestampVersion + '}';
    }

}
